"""Dashboard BI Orbit Nusantara Analitika (Streamlit).

Membaca schema mart dan ml di dw_orbit dengan user read-only `bi_reader`.
"""
import streamlit as st

st.set_page_config(page_title="Orbit Launch Intelligence", page_icon="🚀", layout="wide")

pages = {
    "Bisnis": [
        st.Page("views/ringkasan.py", title="Ringkasan Eksekutif", icon="📊", default=True),
        st.Page("views/keandalan.py", title="Keandalan Peluncuran", icon="🛡️"),
        st.Page("views/biaya.py", title="Biaya Peluncuran", icon="💰"),
        st.Page("views/jadwal.py", title="Jadwal & Risiko", icon="🗓️"),
    ],
    "Analitik & Operasional": [
        st.Page("views/model.py", title="Model Machine Learning", icon="🤖"),
        st.Page("views/pipeline.py", title="Pipeline & Kualitas Data", icon="⚙️"),
    ],
}
nav = st.navigation(pages)
with st.sidebar:
    st.markdown("### 🚀 PT Orbit Nusantara Analitika")
    st.caption("Launch Intelligence: Data Warehouse peluncuran roket (arsip internal 1957–2022 + "
               "Launch Library 2 API).")
    if st.button("🔄 Muat ulang data", width="stretch"):
        st.cache_data.clear()
        st.rerun()
nav.run()
