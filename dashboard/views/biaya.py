import plotly.express as px
import streamlit as st

from data import biaya, peluncuran
from ui import insight, kpis, pct, source_note, style

st.title("💰 Biaya Peluncuran")
st.caption("Perkiraan biaya roket yang masih aktif, hubungannya dengan tingkat keberhasilan, dan kelengkapan data harga.")

b = biaya()
f1, f2, f3 = st.columns(3)
status = f1.segmented_control("Status roket", ["Active", "Inactive"], default="Active")
min_n = f2.number_input("Min. peluncuran roket", 0, 500, 5)
prod = f3.multiselect("Produsen", sorted(b.produsen.dropna().unique()))
d = b[(b.total_peluncuran >= min_n)]
if status:
    d = d[d.status_roket == status]
if prod:
    d = d[d.produsen.isin(prod)]
dc = d.dropna(subset=["perkiraan_biaya_musd"])

kpis([("Roket (filter)", f"{len(d)}", None, None),
      ("Ada perkiraan biaya", f"{len(dc)} ({len(dc) / max(len(d), 1):.0%})", None, "launch_cost LL2 tidak kosong"),
      ("Median biaya", f"${dc.perkiraan_biaya_musd.median():,.0f} jt" if len(dc) else "–", None, "juta USD"),
      ("Snapshot", str(b.snapshot_tanggal.max()) if not b.empty else "–", None, "fact_konfigurasi_roket terbaru")])

c1, c2 = st.columns([3, 2])
c1.plotly_chart(style(px.scatter(dc, x="perkiraan_biaya_musd", y="tingkat_keberhasilan_ll2", size="total_peluncuran",
                                 color="produsen", hover_name="nama_roket", log_x=True, size_max=40,
                                 title="Biaya vs tingkat keberhasilan (ukuran = jumlah peluncuran)",
                                 labels={"perkiraan_biaya_musd": "Perkiraan biaya (juta USD, log)",
                                         "tingkat_keberhasilan_ll2": "Keberhasilan (%)"}), 460, legend=False),
                width="stretch")
dc = dc.assign(biaya_per_sukses=dc.perkiraan_biaya_musd / (dc.tingkat_keberhasilan_ll2 / 100))
c2.markdown("**Roket teruji (≥ 20 peluncuran) dengan biaya terendah**")
c2.dataframe(dc[dc.total_peluncuran >= 20].sort_values("perkiraan_biaya_musd")
             [["nama_roket", "produsen", "perkiraan_biaya_musd", "tingkat_keberhasilan_ll2", "total_peluncuran"]]
             .head(12), hide_index=True, width="stretch",
             column_config={"perkiraan_biaya_musd": st.column_config.NumberColumn("Biaya (jt USD)", format="%.1f"),
                            "tingkat_keberhasilan_ll2": st.column_config.NumberColumn("Sukses %", format="%.1f")})

st.subheader("Kelengkapan data harga pada peluncuran historis")
p = peluncuran()
comp = (p.assign(ada_harga=p.harga_musd.notna(), dekade=(p.tahun // 10 * 10).astype(str) + "-an")
        .groupby("dekade").agg(peluncuran=("ada_harga", "size"), kelengkapan=("ada_harga", "mean")).reset_index())
st.plotly_chart(style(px.bar(comp, x="dekade", y="kelengkapan", text_auto=".0%", hover_data=["peluncuran"],
                             title="Proporsi peluncuran yang memiliki data harga per dekade"), 300, legend=False)
                .update_yaxes(tickformat=".0%"), width="stretch")

if len(dc[dc.total_peluncuran >= 20]):
    best = dc[dc.total_peluncuran >= 20].sort_values("biaya_per_sukses").iloc[0]
    corr = dc[["perkiraan_biaya_musd", "tingkat_keberhasilan_ll2"]].corr(method="spearman").iloc[0, 1]
    insight("Roket mahal tidak otomatis lebih andal",
            f"Korelasi Spearman biaya vs keberhasilan pada {len(dc)} roket = {corr:+.2f}. Biaya per peluncuran "
            f"sukses terendah di antara roket teruji: {best.nama_roket} (${best.perkiraan_biaya_musd:,.0f} jt, "
            f"{best.tingkat_keberhasilan_ll2:.1f}% sukses). Data harga historis hanya lengkap "
            f"{pct(p.harga_musd.notna().mean(), 0)} dari seluruh peluncuran.",
            "Keandalan lebih ditentukan oleh kematangan & kadensi roket daripada harganya; harga yang tersedia "
            "adalah perkiraan per jenis roket, bukan harga kontrak.",
            "Negosiasi yang hanya bertumpu pada harga berisiko memilih roket murah yang belum teruji, atau membayar "
            "premium tanpa tambahan keandalan.",
            "Gunakan metrik 'biaya per peluncuran sukses' sebagai acuan negosiasi, dan cantumkan sumber & tahun "
            "harga di setiap penawaran karena kelengkapan data harga terbatas.",
            "Divisi Keuangan dan Pengadaan")
source_note("mart.v_biaya_roket (fact_konfigurasi_roket snapshot terbaru), mart.v_peluncuran")
