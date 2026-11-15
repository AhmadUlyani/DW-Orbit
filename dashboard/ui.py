"""Komponen UI bersama: KPI, kotak insight (Data -> Temuan -> Interpretasi -> Impact -> Rekomendasi)."""
import plotly.io as pio
import streamlit as st

PALETTE = ["#1f4e79", "#2e86c1", "#f39c12", "#27ae60", "#8e44ad", "#c0392b", "#7f8c8d", "#16a085"]
RISK_COLORS = {"Rendah": "#27ae60", "Sedang": "#f39c12", "Tinggi": "#c0392b", "None": "#bdc3c7"}
pio.templates.default = "plotly_white"


def style(fig, height=360, legend=True):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=40, b=10), colorway=PALETTE,
                      legend=dict(orientation="h", y=-0.2) if legend else None, showlegend=legend,
                      font=dict(size=12))
    return fig


def kpis(items):
    cols = st.columns(len(items))
    for c, (label, value, delta, help_) in zip(cols, items):
        c.metric(label, value, delta, help=help_, border=True)


def insight(title, temuan, interpretasi, impact, rekomendasi, stakeholder=None):
    with st.container(border=True):
        st.markdown(f"**💡 {title}**" + (f"  \n<small>Stakeholder: {stakeholder}</small>" if stakeholder else ""),
                    unsafe_allow_html=True)
        st.markdown(f"- **Temuan:** {temuan}\n- **Interpretasi:** {interpretasi}\n"
                    f"- **Dampak:** {impact}\n- **Rekomendasi:** {rekomendasi}")


def pct(x, d=1):
    return "–" if x is None or x != x else f"{x * 100:.{d}f}%"


def source_note(text):
    st.caption(f"Sumber data: {text}")
