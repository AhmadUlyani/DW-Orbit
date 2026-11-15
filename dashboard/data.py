"""Akses data dashboard: schema mart & ml lewat user read-only bi_reader."""
import os
from decimal import Decimal

import pandas as pd
import psycopg2
import streamlit as st


@st.cache_resource
def _conn_params():
    return dict(host=os.getenv("DW_HOST", "localhost"), port=int(os.getenv("DW_PORT", "5433")),
                dbname=os.getenv("DW_DB", "dw_orbit"), user=os.getenv("DW_BI_USER", "bi_reader"),
                password=os.getenv("DW_BI_PASSWORD", ""), connect_timeout=10,
                application_name="orbit-dashboard")


@st.cache_data(ttl=600, show_spinner=False)
def q(sql: str, params: tuple | None = None) -> pd.DataFrame:
    conn = psycopg2.connect(**_conn_params())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            df = pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()
    # NUMERIC dibaca psycopg2 sebagai Decimal; ubah ke float supaya bisa dihitung bersama float
    for c in df.columns[df.dtypes == object]:
        s = df[c].dropna()
        if len(s) and isinstance(s.iloc[0], Decimal):
            df[c] = pd.to_numeric(df[c])
    return df


def peluncuran():
    df = q("""SELECT tanggal, tahun, bulan, nama_perusahaan, jenis_perusahaan, nama_roket, status_roket, situs,
                     kode_negara, orbit, kelompok_orbit, status_misi, is_sukses, is_gagal, nama_misi, jenis_misi,
                     alasan_gagal, harga_musd, sumber_harga, sumber_data
              FROM mart.v_peluncuran""")
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    df["harga_musd"] = pd.to_numeric(df["harga_musd"])
    return df


def jadwal():
    df = q("SELECT * FROM mart.v_jadwal_terkini")
    for c in ("net_waktu",):
        df[c] = pd.to_datetime(df[c], utc=True)
    for c in ("keberhasilan_5th", "perkiraan_biaya_musd", "p_gagal", "geser_jam", "total_geser_jam"):
        df[c] = pd.to_numeric(df[c])
    return df


def biaya():
    df = q("SELECT * FROM mart.v_biaya_roket")
    for c in ("perkiraan_biaya_musd", "tingkat_keberhasilan_ll2", "harga_rata_arsip_musd"):
        df[c] = pd.to_numeric(df[c])
    return df


def champion(model_name):
    return q("""SELECT * FROM ml.model_registry WHERE model_name=%s AND version =
                (SELECT version FROM ml.model_registry WHERE model_name=%s AND is_champion
                 ORDER BY trained_at DESC LIMIT 1)""", (model_name, model_name))
