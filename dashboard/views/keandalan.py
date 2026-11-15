import plotly.express as px
import streamlit as st

from data import peluncuran
from ui import insight, kpis, pct, source_note, style

st.title("🛡️ Keandalan Peluncuran")
st.caption("Penyedia jasa dan roket mana yang paling andal, serta bagaimana tren keberhasilan dan jenis kegagalannya.")

df = peluncuran()
f1, f2, f3 = st.columns([2, 2, 1])
y0, y1 = f1.slider("Periode", int(df.tahun.min()), int(df.tahun.max()), (2015, int(df.tahun.max())))
jenis = f2.multiselect("Jenis perusahaan", sorted(df.jenis_perusahaan.dropna().unique()))
min_n = f3.number_input("Min. peluncuran", 1, 200, 10)
d = df[df.tahun.between(y0, y1)]
if jenis:
    d = d[d.jenis_perusahaan.isin(jenis)]

prov = (d.groupby("nama_perusahaan").agg(peluncuran=("is_sukses", "size"), sukses=("is_sukses", "sum"),
                                         gagal=("is_gagal", "sum")).reset_index())
prov["tingkat_keberhasilan"] = prov.sukses / prov.peluncuran
prov = prov[prov.peluncuran >= min_n].sort_values("tingkat_keberhasilan")
kpis([("Peluncuran", f"{len(d):,}", None, None), ("Berhasil", pct(d.is_sukses.mean()), None, None),
      ("Gagal (semua jenis)", f"{int(d.is_gagal.sum()):,}", None, "Failure + Partial + Prelaunch"),
      ("Penyedia (≥ min.)", f"{len(prov)}", None, None)])

c1, c2 = st.columns([3, 2])
c1.plotly_chart(style(px.bar(prov.tail(20), x="tingkat_keberhasilan", y="nama_perusahaan", orientation="h",
                             hover_data=["peluncuran", "gagal"], text_auto=".1%",
                             title=f"Tingkat keberhasilan per penyedia (≥ {min_n} peluncuran)"), 480, legend=False)
                .update_xaxes(tickformat=".0%"), width="stretch")
ft = d[d.is_gagal == 1].groupby(["tahun", "status_misi"]).size().reset_index(name="jumlah")
c2.plotly_chart(style(px.bar(ft, x="tahun", y="jumlah", color="status_misi", title="Jenis kegagalan per tahun"),
                      480), width="stretch")

pick = st.multiselect("Bandingkan tren penyedia", prov.sort_values("peluncuran").nama_perusahaan.tail(30)[::-1],
                      default=list(prov.sort_values("peluncuran").nama_perusahaan.tail(4)[::-1]))
if pick:
    tr = (d[d.nama_perusahaan.isin(pick)].groupby(["tahun", "nama_perusahaan"])
          .agg(n=("is_sukses", "size"), s=("is_sukses", "mean")).reset_index())
    st.plotly_chart(style(px.line(tr, x="tahun", y="s", color="nama_perusahaan", markers=True, hover_data=["n"],
                                  title="Tren tingkat keberhasilan per tahun")).update_yaxes(tickformat=".0%"),
                    width="stretch")

st.subheader("Keandalan per roket")
rk = (d.groupby(["nama_roket", "nama_perusahaan"]).agg(peluncuran=("is_sukses", "size"), sukses=("is_sukses", "sum"),
                                                       terakhir=("tanggal", "max")).reset_index())
rk["tingkat_keberhasilan"] = rk.sukses / rk.peluncuran
st.dataframe(rk[rk.peluncuran >= min_n].sort_values(["tingkat_keberhasilan", "peluncuran"], ascending=False),
             width="stretch", hide_index=True,
             column_config={"tingkat_keberhasilan": st.column_config.ProgressColumn("Keberhasilan", format="percent",
                                                                                   min_value=0, max_value=1),
                            "terakhir": st.column_config.DateColumn("Peluncuran terakhir")})

# insight: pengaruh pengalaman roket terhadap kegagalan (dihitung dari seluruh riwayat)
h = df.sort_values("tanggal").copy()
h["ke"] = h.groupby("nama_roket").cumcount() + 1
early, mature = h[h.ke <= 3].is_gagal.mean(), h[h.ke > 10].is_gagal.mean()
rec = df[df.tahun >= int(df.tahun.max()) - 4]
rp = rec.groupby("nama_perusahaan").agg(n=("is_sukses", "size"), s=("is_sukses", "mean"))
rp = rp[rp.n >= 20].sort_values("s", ascending=False)
insight("Pengalaman roket adalah penentu utama risiko",
        f"Tingkat gagal pada 3 penerbangan pertama sebuah roket {early:.0%}, sedangkan setelah penerbangan ke-10 "
        f"hanya {mature:.0%} ({early / mature:.1f}× lebih rendah). Penyedia ≥20 peluncuran paling andal 5 tahun "
        f"terakhir: {', '.join(f'{i} ({v:.0%})' for i, v in rp.s.head(3).items())}.",
        "Roket baru masih dalam fase pematangan desain; keandalan naik tajam setelah beberapa penerbangan.",
        "Menempatkan muatan klien pada roket baru menaikkan risiko kehilangan satelit & biaya asuransi.",
        "Untuk muatan bernilai tinggi, syaratkan roket dengan ≥10 penerbangan; roket baru hanya untuk muatan "
        "berbiaya rendah/eksperimental dengan diskon harga atau asuransi tambahan.",
        "Divisi Analisis Misi")
source_note("mart.v_peluncuran (fact_peluncuran + dim_perusahaan, dim_roket, dim_status, dim_tanggal)")
