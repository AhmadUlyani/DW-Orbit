import json

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data import champion, q
from ui import insight, kpis, source_note, style

st.title("🤖 Model Machine Learning")
st.caption("Baseline → model → perbandingan → model terpilih. Semua model dilatih dari Data Warehouse (mart.v_peluncuran).")

tab1, tab2, tab3 = st.tabs(["Risiko kegagalan (classification)", "Forecasting peluncuran", "Data drift"])

LABEL = {"baseline_prior": "B0 Baseline prior", "baseline_provider": "B1 Baseline rate penyedia",
         "logreg": "M1 Logistic Regression", "random_forest": "M2 Random Forest", "hist_gb": "M3 HistGradientBoosting",
         "naive_last": "B0 Naive (bulan lalu)", "seasonal_naive": "B1 Seasonal naive (t-12)",
         "moving_avg_12": "B2 Moving average 12", "ridge_lags": "M1 Ridge (lag)", "hist_gb_lags": "M2 HistGB (lag)"}


def _metrics(reg):
    rows = []
    for r in reg.itertuples():
        m = r.metrics if isinstance(r.metrics, dict) else json.loads(r.metrics)
        rows.append({"kandidat": LABEL.get(r.candidate, r.candidate), "candidate": r.candidate,
                     "champion": "🏆" if r.is_champion else "", **m, "params": r.params})
    return pd.DataFrame(rows)


with tab1:
    reg = champion("launch_risk")
    if reg.empty:
        st.info("Model belum dilatih. Jalankan DAG orbit_ml_training.")
    else:
        v = reg.version.iloc[0]
        m = _metrics(reg)
        ch = m[m.champion != ""].iloc[0]
        kpis([("Model terpilih", LABEL.get(ch.candidate, ch.candidate), None, "dipilih dari rata-rata PR-AUC time-series CV"),
              ("ROC-AUC test", f"{ch.roc_auc:.3f}", f"{ch.roc_auc - m[m.candidate == 'baseline_provider'].roc_auc.iloc[0]:+.3f} vs B1", None),
              ("PR-AUC test", f"{ch.pr_auc:.3f}", f"{ch.pr_auc / ch.base_rate:.1f}× base rate", "base rate = tingkat gagal aktual"),
              ("Recall / Precision", f"{ch.recall:.0%} / {ch.precision:.0%}", None, f"ambang {ch.threshold:.3f}"),
              ("Data test", f"{int(ch.n):,} peluncuran", f"gagal {ch.base_rate:.1%}", "2020–sekarang")])
        st.markdown("**Perbandingan kandidat**. CV = rata-rata 4 fold time-series (2000–2019); test = hold-out 2020–sekarang")
        cols = ["champion", "kandidat", "cv_pr_auc", "cv_roc_auc", "roc_auc", "pr_auc", "precision", "recall", "f1",
                "brier", "params"]
        st.dataframe(m[cols].sort_values("cv_pr_auc", ascending=False), hide_index=True, width="stretch",
                     column_config={c: st.column_config.NumberColumn(format="%.3f") for c in cols[2:10]})
        long = m.melt(id_vars="kandidat", value_vars=["cv_pr_auc", "pr_auc", "roc_auc"], var_name="metrik")
        c1, c2 = st.columns(2)
        c1.plotly_chart(style(px.bar(long, x="kandidat", y="value", color="metrik", barmode="group",
                                     title="Baseline vs model (lebih tinggi lebih baik)")), width="stretch")
        bt = q("""SELECT actual_gagal, p_gagal, pred_gagal, tanggal FROM ml.pred_launch_backtest
                  WHERE model_name='launch_risk' AND version=%s AND candidate=%s""", (v, ch.candidate))
        bt["p_gagal"] = bt.p_gagal.astype(float)
        bt["desil"] = pd.qcut(bt.p_gagal.rank(method="first"), 10, labels=range(1, 11))
        cal = bt.groupby("desil", observed=True).agg(prediksi=("p_gagal", "mean"), aktual=("actual_gagal", "mean")).reset_index()
        fig = go.Figure([go.Bar(x=cal.desil.astype(str), y=cal.aktual, name="Tingkat gagal aktual"),
                         go.Scatter(x=cal.desil.astype(str), y=cal.prediksi, name="Rata-rata prediksi", mode="lines+markers")])
        fig.update_layout(title="Actual vs prediction per desil skor (test)", xaxis_title="Desil risiko (1 = terendah)")
        c2.plotly_chart(style(fig).update_yaxes(tickformat=".0%"), width="stretch")

        c3, c4, c5 = st.columns([1, 2, 2])
        cm = np.array([[ch.tn, ch.fp], [ch.fn, ch.tp]])
        c3.plotly_chart(style(px.imshow(cm, text_auto=True, x=["Pred sukses", "Pred gagal"], y=["Aktual sukses", "Aktual gagal"],
                                        color_continuous_scale="Blues", title="Confusion matrix (test)"), 320, legend=False)
                        .update_coloraxes(showscale=False), width="stretch")
        fi = q("""SELECT feature, importance, importance_std FROM ml.feature_importance
                  WHERE model_name='launch_risk' AND version=%s ORDER BY importance DESC""", (v,))
        if not fi.empty:
            c4.plotly_chart(style(px.bar(fi.sort_values("importance"), x="importance", y="feature", orientation="h",
                                         error_x="importance_std", title="Permutation importance (penurunan PR-AUC)"),
                                  320, legend=False), width="stretch")
        yr = q("""SELECT candidate, split, metric, value FROM ml.model_experiment WHERE model_name='launch_risk'
                  AND version=%s AND split LIKE 'test\\_2%%' AND metric='roc_auc'""", (v,))
        if not yr.empty:
            yr["tahun"] = yr.split.str[5:]
            yr["kandidat"] = yr.candidate.map(LABEL)
            c5.plotly_chart(style(px.line(yr, x="tahun", y="value", color="kandidat", markers=True,
                                          title="Stabilitas ROC-AUC per tahun (test)"), 320), width="stretch")
        b1 = m[m.candidate == "baseline_provider"].iloc[0]
        top_f = fi.feature.head(3).tolist() if not fi.empty else []
        insight("Model ML memberi peringatan dini kegagalan jauh lebih baik dari heuristik",
                f"{LABEL.get(ch.candidate)} mencapai ROC-AUC {ch.roc_auc:.2f} dan PR-AUC {ch.pr_auc:.2f} pada data "
                f"2020–sekarang, dibanding baseline rate penyedia {b1.roc_auc:.2f}/{b1.pr_auc:.2f}. Desil risiko "
                f"tertinggi memiliki tingkat gagal aktual {cal.aktual.iloc[-1]:.0%} vs {cal.aktual.iloc[0]:.0%} di "
                f"desil terendah. Fitur terpenting: {', '.join(top_f)}.",
                "PR-AUC jauh di atas base rate berarti model mampu mengurutkan peluncuran berisiko; nilai absolut "
                "probabilitas tetap perkiraan karena kejadian gagal jarang (±8%).",
                f"Dengan ambang terpilih model menangkap {ch.recall:.0%} kegagalan dengan presisi {ch.precision:.0%}, "
                "cukup untuk memprioritaskan due diligence, bukan untuk keputusan otomatis.",
                "Gunakan skor risiko sebagai daftar prioritas pemeriksaan (asuransi, jadwal cadangan) di halaman "
                "Jadwal; latih ulang mingguan dan pantau stabilitas per tahun.",
                "Divisi Analisis Misi")

with tab2:
    reg = champion("launch_forecast")
    if reg.empty:
        st.info("Model forecasting belum dilatih.")
    else:
        v = reg.version.iloc[0]
        m = _metrics(reg)
        st.dataframe(m[["champion", "kandidat", "mae", "rmse", "mape", "r2"]].sort_values("mae"), hide_index=True,
                     width="stretch",
                     column_config={c: st.column_config.NumberColumn(format="%.2f") for c in ["mae", "rmse", "mape", "r2"]})
        fc = q("""SELECT candidate, bulan, actual, predicted, is_future FROM ml.forecast_monthly
                  WHERE model_name='launch_forecast' AND version=%s ORDER BY bulan""", (v,))
        fc["kandidat"] = fc.candidate.map(LABEL)
        bt = fc[~fc.is_future]
        fig = px.line(bt, x="bulan", y="predicted", color="kandidat", title="Actual vs prediksi one-step-ahead (validasi + test)")
        act = bt.drop_duplicates("bulan")
        fig.add_trace(go.Scatter(x=act.bulan, y=act.actual, name="Aktual", mode="lines+markers",
                                 line=dict(color="black", width=3)))
        st.plotly_chart(style(fig, 420), width="stretch")
        ch = m[m.champion != ""].iloc[0]
        sn = m[m.candidate == "seasonal_naive"].iloc[0]
        insight("Forecast kebutuhan kapasitas peluncuran",
                f"Model terpilih {LABEL.get(ch.candidate)}: MAE {ch.mae:.1f} peluncuran/bulan (MAPE {ch.mape:.1f}%) "
                f"pada 24 bulan terakhir, vs seasonal naive MAE {sn.mae:.1f}.",
                "Jumlah peluncuran bulanan sangat dipengaruhi tren beberapa bulan terakhir (momentum kadensi), "
                "bukan pola musiman tahunan.",
                "Proyeksi 12 bulan memberi gambaran kapasitas pasar & potensi antrean slot rideshare.",
                "Pakai forecast + interval 95% untuk perencanaan target penjualan slot per kuartal; tinjau ulang "
                "setiap bulan karena tren industri berubah cepat.", "Direktur Utama")

with tab3:
    dr = q("""SELECT feature, ref_period, cur_period, psi, ref_mean, cur_mean, status FROM ml.drift_report
              WHERE version = (SELECT MAX(version) FROM ml.drift_report) ORDER BY cur_period, psi DESC""")
    if dr.empty:
        st.info("Belum ada laporan drift.")
    else:
        dr["psi"] = dr.psi.astype(float)
        dr["perbandingan"] = dr.ref_period + " → " + dr.cur_period
        st.plotly_chart(style(px.bar(dr, x="psi", y="feature", color="status", facet_col="perbandingan", orientation="h",
                                     color_discrete_map={"Stabil": "#27ae60", "Moderat": "#f39c12", "Signifikan": "#c0392b"},
                                     title="Population Stability Index (PSI) fitur"), 480), width="stretch")
        st.dataframe(dr.drop(columns="perbandingan"), hide_index=True, width="stretch")
        sig = dr[(dr.status == "Signifikan") & dr.cur_period.str.startswith("2020")].feature.tolist()
        insight("Pola peluncuran modern sangat berbeda dari era latih awal",
                f"PSI signifikan (>0,25) antara 1957–2014 dan 2020–sekarang pada: {', '.join(sig[:6])}.",
                "Industri kini didominasi roket berpengalaman tinggi & kadensi sangat tinggi, sehingga distribusi fitur "
                "bergeser jauh (data drift), walaupun hubungan fitur→risiko relatif stabil (lihat stabilitas ROC-AUC per tahun).",
                "Model yang tidak dilatih ulang akan makin bias terhadap kondisi lama.",
                "Pertahankan retraining mingguan (DAG orbit_ml_training) dengan seluruh data terbaru dan pantau PSI; "
                "jika PSI target (is_gagal) ikut signifikan, evaluasi ulang ambang risk band.", "Data Engineer / Analyst")
source_note("ml.model_registry, ml.model_experiment, ml.pred_launch_backtest, ml.feature_importance, "
            "ml.forecast_monthly, ml.drift_report")
