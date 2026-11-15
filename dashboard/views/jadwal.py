import plotly.express as px
import streamlit as st

from data import jadwal, q
from ui import RISK_COLORS, insight, kpis, source_note, style

st.title("🗓️ Jadwal Peluncuran & Risiko")
st.caption("Jadwal mendatang beserta rekam jejak penyedia, perkiraan biaya, dan skor risiko kegagalan dari model ML.")

jd = jadwal()
if jd.empty:
    st.warning("Belum ada snapshot jadwal di Data Warehouse.")
    st.stop()
snap = jd.snapshot_tanggal.max()
f1, f2, f3, f4 = st.columns(4)
rng = f1.segmented_control("Rentang", ["30 hari", "90 hari", "1 tahun", "Semua"], default="90 hari")
days = {"30 hari": 30, "90 hari": 90, "1 tahun": 365, "Semua": 10**6}[rng or "90 hari"]
orb = f2.multiselect("Kelompok orbit", sorted(jd.kelompok_orbit.dropna().unique()))
neg = f3.multiselect("Negara lokasi", sorted(jd.kode_negara.dropna().unique()))
band = f4.multiselect("Risk band", ["Tinggi", "Sedang", "Rendah"])
d = jd[jd.hari_menuju_peluncuran.between(0, days)]
if orb:
    d = d[d.kelompok_orbit.isin(orb)]
if neg:
    d = d[d.kode_negara.isin(neg)]
if band:
    d = d[d.risk_band.isin(band)]

kpis([("Jadwal", f"{len(d):,}", None, f"snapshot {snap}"),
      ("Go for Launch", f"{(d.status_jadwal == 'Go for Launch').sum():,}", None, "Jadwal yang sudah pasti"),
      ("Risiko Tinggi", f"{(d.risk_band == 'Tinggi').sum():,}", None, "10% skor model teratas"),
      ("Penyedia", f"{d.nama_perusahaan.nunique()}", None, None),
      ("Presisi ≤ hari", f"{d.presisi_jadwal.isin(['Second', 'Minute', 'Hour', 'Day']).mean():.0%}" if len(d) else "–",
       None, "Proporsi jadwal yang tanggalnya sudah diketahui sampai level hari")])

show = d.sort_values("net_waktu")[["tanggal_net", "hari_menuju_peluncuran", "nama_perusahaan", "nama_roket",
                                    "nama_misi", "orbit", "situs", "status_jadwal", "presisi_jadwal", "p_gagal",
                                    "risk_band", "faktor_utama", "keberhasilan_5th", "perkiraan_biaya_musd"]]
st.dataframe(show, hide_index=True, width="stretch", height=380,
             column_config={
                 "tanggal_net": st.column_config.DateColumn("Tanggal (NET)"),
                 "hari_menuju_peluncuran": st.column_config.NumberColumn("H-"),
                 "p_gagal": st.column_config.ProgressColumn("P(gagal)", format="%.2f", min_value=0, max_value=1),
                 "keberhasilan_5th": st.column_config.NumberColumn("Sukses penyedia 5 th (%)", format="%.1f"),
                 "perkiraan_biaya_musd": st.column_config.NumberColumn("Biaya (jt USD)", format="%.1f"),
                 "faktor_utama": st.column_config.TextColumn("Faktor risiko", width="large")})

c1, c2 = st.columns(2)
per = d.groupby(["nama_perusahaan", "risk_band"]).size().reset_index(name="jadwal")
c1.plotly_chart(style(px.bar(per, x="jadwal", y="nama_perusahaan", color="risk_band", orientation="h",
                             color_discrete_map=RISK_COLORS, title="Jadwal per penyedia & risk band"), 420)
                .update_yaxes(categoryorder="total ascending"), width="stretch")
c2.plotly_chart(style(px.histogram(d.dropna(subset=["p_gagal"]), x="p_gagal", color="risk_band", nbins=30,
                                   color_discrete_map=RISK_COLORS, title="Distribusi skor risiko"), 420),
                width="stretch")

st.subheader("Pergeseran jadwal antar snapshot")
pg = q("SELECT * FROM mart.v_pergeseran_jadwal")
if pg.empty or pg.jadwal_terbanding.sum() == 0:
    st.info(f"Baru tersedia {pg.snapshot_tanggal.nunique() if not pg.empty else 0} snapshot harian. Pergeseran jadwal "
            "(geser_jam) dihitung mulai snapshot kedua; grafik ini terisi otomatis seiring ETL harian berjalan.")
else:
    agg = pg.groupby("nama_perusahaan")[["jadwal_terbanding", "jadwal_bergeser"]].sum()
    agg["persen_bergeser"] = agg.jadwal_bergeser / agg.jadwal_terbanding
    agg = agg[agg.jadwal_terbanding >= 5].sort_values("persen_bergeser").tail(15).reset_index()
    st.plotly_chart(style(px.bar(agg, x="persen_bergeser", y="nama_perusahaan", orientation="h", text_auto=".0%",
                                 title="Penyedia dengan jadwal paling sering bergeser"), 380, legend=False)
                    .update_xaxes(tickformat=".0%"), width="stretch")

near = jd[jd.hari_menuju_peluncuran.between(0, 90)]
hi = near[near.risk_band == "Tinggi"]
firm = near.presisi_jadwal.isin(["Second", "Minute", "Hour", "Day"]).mean() if len(near) else 0
top_hi = hi.nama_roket.value_counts().head(3)
insight("Jadwal 90 hari ke depan: siapa yang aman ditawarkan ke klien",
        f"Dari {len(near)} jadwal 90 hari ke depan, {len(hi)} berisiko Tinggi menurut model"
        + (f" (terutama {', '.join(f'{k} ({v})' for k, v in top_hi.items())})" if len(top_hi) else "")
        + f"; hanya {firm:.0%} jadwal yang tanggalnya pasti sampai level hari.",
        "Risiko tinggi didominasi roket baru / penerbangan awal dan penyedia dengan riwayat gagal terbaru; "
        "jadwal tanpa presisi hari masih berupa target, bukan slot pasti.",
        "Menawarkan slot berisiko tinggi atau berpresisi rendah ke klien CubeSat/operator satelit dapat "
        "menunda misi dan menurunkan kepercayaan klien.",
        "Prioritaskan penawaran jadwal berstatus Go for Launch dengan risk band Rendah; jadwal Tinggi hanya "
        "ditawarkan sebagai opsi cadangan disertai klausul penjadwalan ulang.",
        "Divisi Business Development dan Hubungan Klien")
source_note("mart.v_jadwal_terkini (fact_jadwal_snapshot + fact_peluncuran + fact_konfigurasi_roket + "
            "ml.pred_launch_risk), mart.v_pergeseran_jadwal")
