import pandas as pd
import plotly.express as px
import streamlit as st

from data import q
from ui import kpis, source_note, style

st.title("⚙️ Pipeline, Kualitas Data & Reliability")
st.caption("Monitoring operasional untuk Divisi IT dan Data: status pipeline, quality gate, Data Lake, backup/DR, "
           "dan uji skalabilitas. Diakses melalui view mart (schema meta tidak dibuka ke dashboard).")

runs = q("SELECT * FROM mart.v_pipeline_run ORDER BY batch_id DESC LIMIT 2000")
runs["started_at"] = pd.to_datetime(runs.started_at, utc=True)
last24 = runs[runs.started_at >= pd.Timestamp.utcnow() - pd.Timedelta(days=1)]
gate = q("SELECT * FROM mart.v_quality_gate ORDER BY gate_id DESC")
lake = q("SELECT * FROM mart.v_lake_ringkas ORDER BY zone, dataset")
bk = q("SELECT * FROM mart.v_backup ORDER BY backup_id DESC")
rt = q("SELECT * FROM mart.v_restore_test ORDER BY test_id DESC")
kpis([("Task 24 jam", f"{len(last24)}", None, None),
      ("Sukses 24 jam", f"{(last24.status.isin(['SUCCESS', 'SKIPPED'])).mean():.0%}" if len(last24) else "–", None,
       "SUCCESS/SKIPPED dari seluruh task"),
      ("Quality gate PASS", f"{(gate.decision == 'PASS').mean():.0%}" if len(gate) else "–", None, None),
      ("Objek Data Lake", f"{int(lake.objek.sum()):,}" if len(lake) else "0",
       f"{lake.total_bytes.sum() / 1e6:,.1f} MB" if len(lake) else None, None),
      ("Backup terakhir", str(pd.to_datetime(bk.started_at.iloc[0]).strftime('%d %b %H:%M')) if len(bk) else "–", None, None),
      ("RTO uji restore", f"{float(rt.duration_s.iloc[0]):.1f} s" if len(rt) else "–", None, "target ≤ 4 jam")])

t1, t2, t3, t4, t5 = st.tabs(["Pipeline run", "Data quality", "Data Lake & lineage", "Backup & DR", "Skalabilitas"])
with t1:
    st.dataframe(runs.head(200), hide_index=True, width="stretch", height=320)
    dur = runs[runs.status == "SUCCESS"].groupby(["pipeline", "task"]).agg(
        rata_durasi_s=("duration_s", lambda s: float(pd.to_numeric(s).mean())), run=("batch_id", "count"),
        baris=("rows_written", "sum")).reset_index()
    st.plotly_chart(style(px.bar(dur.sort_values("rata_durasi_s"), x="rata_durasi_s", y="task", color="pipeline",
                                 orientation="h", title="Rata-rata durasi task (detik)"), 460), width="stretch")
    al = q("SELECT level, dag_id, task_id, message, created_at FROM mart.v_alert ORDER BY alert_id DESC LIMIT 50")
    st.markdown("**Alert terbaru** (callback on_failure / on_retry / quality gate)")
    st.dataframe(al, hide_index=True, width="stretch", height=220)
with t2:
    st.markdown("**Keputusan Quality Gate per batch** (BLOCK bila reject > 5% atau ada pemeriksaan ERROR gagal)")
    st.dataframe(gate.head(50), hide_index=True, width="stretch", height=220)
    dq = q("""SELECT DISTINCT ON (table_name, rule_id, check_name) table_name, rule_id, check_name, dimension, severity,
                     status, failed_rows, total_rows, fail_pct, checked_at
              FROM mart.v_kualitas_data ORDER BY table_name, rule_id, check_name, checked_at DESC""")
    dq["fail_pct"] = pd.to_numeric(dq.fail_pct)
    c1, c2 = st.columns([2, 1])
    c1.dataframe(dq, hide_index=True, width="stretch", height=420,
                 column_config={"fail_pct": st.column_config.NumberColumn("% gagal", format="%.2f")})
    dim = dq.groupby(["dimension", "status"]).size().reset_index(name="pemeriksaan")
    c2.plotly_chart(style(px.bar(dim, x="pemeriksaan", y="dimension", color="status", orientation="h",
                                 color_discrete_map={"PASS": "#27ae60", "FAIL": "#c0392b"},
                                 title="Pemeriksaan per dimensi kualitas"), 420), width="stretch")
    st.markdown("**Quarantine (staging.stg_reject)**")
    st.dataframe(q("SELECT * FROM mart.v_reject_ringkas ORDER BY jumlah DESC"), hide_index=True, width="stretch")
with t3:
    st.dataframe(lake, hide_index=True, width="stretch")
    lin = q("""SELECT batch_id, target_layer, target_table, rules_applied, rows_affected, created_at,
                      array_length(source_refs, 1) AS jumlah_sumber, source_refs[1] AS contoh_sumber
               FROM mart.v_lineage ORDER BY lineage_id DESC LIMIT 100""")
    st.markdown("**Data lineage**: sumber → aturan transformasi → tabel tujuan")
    st.dataframe(lin, hide_index=True, width="stretch", height=300)
    st.markdown("**Katalog data (business metadata)**")
    st.dataframe(q("SELECT * FROM mart.v_data_catalog"), hide_index=True, width="stretch", height=300)
with t4:
    st.dataframe(bk.head(30), hide_index=True, width="stretch")
    st.markdown("**Uji restore (DR drill)**")
    st.dataframe(rt.drop(columns=["details"], errors="ignore"), hide_index=True, width="stretch")
with t5:
    pf = q("SELECT * FROM mart.v_perf_result WHERE run_label = (SELECT run_label FROM mart.v_perf_result "
           "ORDER BY perf_id DESC LIMIT 1) ORDER BY perf_id")
    if pf.empty:
        st.info("Belum ada hasil uji skalabilitas (DAG orbit_perf_test).")
    else:
        for c in ("rows", "duration_s", "throughput_rps", "scale_factor"):
            pf[c] = pd.to_numeric(pf[c])
        st.caption(f"Run {pf.run_label.iloc[0]}: data sintetis berlabel di schema perf (tidak masuk DWH analitik).")
        c1, c2 = st.columns(2)
        etl = pf[pf.test_name.isin(["T1_transform_throughput", "T2_lake_write_parquet", "T3_staging_bulk_insert",
                                    "T4_fact_load_sql"])]
        c1.plotly_chart(style(px.line(etl, x="rows", y="duration_s", color="test_name", markers=True, log_x=True,
                                      title="Jumlah baris vs durasi tahap pipeline")), width="stretch")
        qq = pf[pf.test_name.str.startswith(("T5", "T6"))].assign(latency_ms=lambda x: x.duration_s * 1000)
        c2.plotly_chart(style(px.bar(qq, x="rows", y="latency_ms", color="stage", barmode="group", facet_col="test_name",
                                     log_x=False, title="Latensi query: tanpa vs dengan index (ms)")),
                        width="stretch")
        st.dataframe(pf, hide_index=True, width="stretch")
source_note("mart.v_pipeline_run, v_quality_gate, v_kualitas_data, v_reject_ringkas, v_lake_ringkas, v_lineage, "
            "v_backup, v_restore_test, v_perf_result, v_alert, v_data_catalog")
