import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from data import champion, jadwal, peluncuran, q
from ui import insight, kpis, pct, source_note, style

st.title("📊 Ringkasan Eksekutif")
st.caption("Kinerja & pangsa penyedia jasa peluncuran dari tahun ke tahun, jadwal dalam waktu dekat, "
           "dan proyeksi jumlah peluncuran.")

df = peluncuran()
jd = jadwal()
last_full = int(df.tahun.max()) - (1 if df.tanggal.max().month < 12 else 0)
y0, y1 = st.slider("Periode (tahun)", int(df.tahun.min()), int(df.tahun.max()), (2010, int(df.tahun.max())))
d = df[df.tahun.between(y0, y1)]

n_last, n_prev = (df.tahun == last_full).sum(), (df.tahun == last_full - 1).sum()
up90 = jd[jd.hari_menuju_peluncuran.between(0, 90)] if not jd.empty else jd
kpis([
    ("Peluncuran (periode)", f"{len(d):,}", None, "Jumlah peluncuran pada periode terpilih"),
    ("Tingkat keberhasilan", pct(d.is_sukses.mean()), None, "Status Success / seluruh peluncuran"),
    (f"Peluncuran {last_full}", f"{n_last:,}", f"{(n_last / n_prev - 1) * 100:+.0f}% vs {last_full - 1}" if n_prev else None,
     "Pertumbuhan tahunan (tahun penuh terakhir)"),
    ("Jadwal 90 hari ke depan", f"{len(up90):,}", None, "Snapshot jadwal terbaru, net 0–90 hari"),
    ("Jadwal risiko Tinggi", f"{(up90.risk_band == 'Tinggi').sum():,}" if not up90.empty else "–", None,
     "Skor model ML (10% teratas)"),
])

c1, c2 = st.columns([3, 2])
top = d.nama_perusahaan.value_counts().head(6).index
by = (d.assign(penyedia=d.nama_perusahaan.where(d.nama_perusahaan.isin(top), "Lainnya"))
      .groupby(["tahun", "penyedia"]).size().reset_index(name="peluncuran"))
c1.plotly_chart(style(px.bar(by, x="tahun", y="peluncuran", color="penyedia",
                             title="Peluncuran per tahun per penyedia (6 teratas)")), width="stretch")
share = df[df.tahun == last_full].nama_perusahaan.value_counts(normalize=True).head(8).reset_index()
share.columns = ["penyedia", "pangsa"]
c2.plotly_chart(style(px.bar(share.sort_values("pangsa"), x="pangsa", y="penyedia", orientation="h",
                             title=f"Pangsa peluncuran {last_full}", text_auto=".0%"), legend=False),
                width="stretch")

# forecast
st.subheader("Proyeksi jumlah peluncuran bulanan (model forecasting)")
reg = champion("launch_forecast")
if reg.empty:
    st.info("Model forecasting belum dilatih (DAG orbit_ml_training).")
    ch = None
else:
    ch = reg[reg.is_champion].iloc[0]
    fc = q("""SELECT bulan, actual, predicted, lower_pi, upper_pi, is_future FROM ml.forecast_monthly
              WHERE model_name='launch_forecast' AND version=%s AND candidate=%s ORDER BY bulan""",
           (ch.version, ch.candidate))
    fc["bulan"] = pd.to_datetime(fc.bulan)
    hist = df.groupby(df.tanggal.dt.to_period("M").dt.to_timestamp()).size()
    hist = hist[hist.index >= "2018-01-01"]
    hist = hist[hist.index < pd.Timestamp.today().to_period("M").to_timestamp()]
    fut = fc[fc.is_future]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=hist.index, y=hist.values, name="Aktual", line=dict(color="#1f4e79")))
    bt = fc[~fc.is_future]
    fig.add_trace(go.Scatter(x=bt.bulan, y=bt.predicted, name=f"Prediksi backtest ({ch.candidate})",
                             line=dict(color="#f39c12", dash="dot")))
    fig.add_trace(go.Scatter(x=list(fut.bulan) + list(fut.bulan[::-1]),
                             y=list(fut.upper_pi) + list(fut.lower_pi[::-1]), fill="toself",
                             fillcolor="rgba(243,156,18,0.15)", line=dict(width=0), name="Interval 95%"))
    fig.add_trace(go.Scatter(x=fut.bulan, y=fut.predicted, name="Forecast 12 bulan", line=dict(color="#c0392b")))
    st.plotly_chart(style(fig, 340), width="stretch")
    m = pd.Series(ch.metrics if isinstance(ch.metrics, dict) else json.loads(ch.metrics))
    st.caption(f"Model terpilih: **{ch.candidate}**, test 24 bulan: MAE {m.get('mae', float('nan')):.1f} "
               f"peluncuran/bulan, MAPE {m.get('mape', float('nan')):.1f}%. Detail di halaman Model ML.")

# insight
recent = df[df.tahun >= last_full - 4]
lead = recent.nama_perusahaan.value_counts(normalize=True)
lead_name, lead_share = lead.index[0], lead.iloc[0]
growth = (n_last / (df.tahun == last_full - 4).sum() - 1) if (df.tahun == last_full - 4).sum() else 0
insight("Konsentrasi pasar penyedia jasa peluncuran",
        f"Dalam 5 tahun terakhir ({last_full - 4}–{last_full}) {lead_name} melakukan {lead_share:.0%} dari seluruh "
        f"peluncuran; jumlah peluncuran {last_full} berubah {growth:+.0%} dibanding {last_full - 4}.",
        "Pertumbuhan industri didorong oleh satu-dua penyedia dengan roket reusable dan kadensi tinggi, "
        "sedangkan penyedia lain tumbuh jauh lebih lambat.",
        "Ketersediaan slot & daya tawar harga sangat bergantung pada penyedia dominan; risiko konsentrasi "
        "(antrean, kebijakan ekspor AS) bagi klien Perusahaan.",
        f"Jadikan {lead_name} mitra utama untuk misi LEO rideshare, tetapi pertahankan minimal dua penyedia "
        "alternatif (mis. Rocket Lab / ISRO / Arianespace) di setiap proposal untuk mitigasi risiko jadwal.",
        "Direktur Utama, Business Development")
source_note("mart.v_peluncuran, mart.v_jadwal_terkini, ml.forecast_monthly")
