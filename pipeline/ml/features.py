"""Feature engineering dari Data Warehouse (mart.v_peluncuran) untuk model risiko kegagalan.

Fitur dihitung point-in-time: peluncuran ke-i hanya memakai peluncuran sebelumnya (expanding window +
shift), jadi tidak ada kebocoran data masa depan.

Fitur:
  roket_n_prior            jumlah peluncuran roket ini sebelumnya (pengalaman roket)
  roket_fail_rate_prior    tingkat gagal roket sebelumnya, dihaluskan Bayesian ke rata-rata industri
  roket_is_maiden          penerbangan perdana (n_prior = 0)
  roket_is_early           3 penerbangan pertama (n_prior < 3)
  roket_age_years          umur roket sejak penerbangan pertamanya
  roket_days_since_last    jeda sejak penerbangan roket sebelumnya (log1p hari)
  prov_n_prior             jumlah peluncuran penyedia sebelumnya
  prov_fail_rate_prior     tingkat gagal penyedia sebelumnya (dihaluskan)
  prov_fail_last10         jumlah gagal pada 10 peluncuran terakhir penyedia
  prov_days_since_last     jeda sejak peluncuran penyedia sebelumnya (log1p hari)
  industry_launches_365d   jumlah peluncuran global 365 hari sebelumnya (aktivitas industri)
  tahun                    tren teknologi
  jenis_perusahaan, kode_negara (kategorikal, negara jarang -> 'OTHER')
"""
import numpy as np
import pandas as pd

from .. import db

PRIOR_STRENGTH = 5.0           # kekuatan prior Bayesian (setara 5 peluncuran)
NUM_FEATURES = ["roket_n_prior", "roket_fail_rate_prior", "roket_is_maiden", "roket_is_early", "roket_age_years",
                "roket_days_since_last", "prov_n_prior", "prov_fail_rate_prior", "prov_fail_last10",
                "prov_days_since_last", "industry_launches_365d", "tahun"]
CAT_FEATURES = ["jenis_perusahaan", "kode_negara"]
FEATURES = NUM_FEATURES + CAT_FEATURES
TARGET = "is_gagal"


def load_history():
    df = db.read_sql("""SELECT sumber_id, tanggal, waktu_peluncuran, nama_perusahaan, jenis_perusahaan, nama_roket,
                               kode_negara, is_gagal, sumber_data, status_misi
                        FROM mart.v_peluncuran""")
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    df["ts"] = df["tanggal"] + pd.to_timedelta(df["waktu_peluncuran"].astype(str).replace("None", "00:00:00"))
    return df.sort_values(["ts", "sumber_id"]).reset_index(drop=True)


def _group_prior(df, key, prefix, global_rate):
    g = df.groupby(key, sort=False)
    n_prior = g.cumcount()
    fails_prior = g["is_gagal"].cumsum() - df["is_gagal"]
    df[f"{prefix}_n_prior"] = n_prior
    df[f"{prefix}_fail_rate_prior"] = (fails_prior + PRIOR_STRENGTH * global_rate) / (n_prior + PRIOR_STRENGTH)
    prev_ts = g["ts"].shift(1)
    df[f"{prefix}_days_since_last"] = np.log1p((df["ts"] - prev_ts).dt.days.fillna(3650).clip(0, 3650))
    return g


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """Fitur point-in-time untuk seluruh riwayat (df terurut waktu)."""
    df = df.copy()
    # rata-rata industri sampai sebelum peluncuran ini (prior Bayesian)
    global_rate = (df["is_gagal"].cumsum() - df["is_gagal"]).div(np.arange(len(df)).clip(min=1)).fillna(0.1)
    global_rate.iloc[0] = 0.1
    g_r = _group_prior(df, "nama_roket", "roket", global_rate)
    g_p = _group_prior(df, "nama_perusahaan", "prov", global_rate)
    df["roket_is_maiden"] = (df["roket_n_prior"] == 0).astype(int)
    df["roket_is_early"] = (df["roket_n_prior"] < 3).astype(int)
    df["roket_age_years"] = (df["ts"] - g_r["ts"].transform("min")).dt.days / 365.25
    df["prov_fail_last10"] = g_p["is_gagal"].transform(lambda s: s.shift(1).rolling(10, min_periods=1).sum()).fillna(0)
    ts = df["ts"].values
    df["industry_launches_365d"] = np.searchsorted(ts, ts, side="left") - \
        np.searchsorted(ts, ts - np.timedelta64(365, "D"), side="left")
    df["tahun"] = df["ts"].dt.year
    df["jenis_perusahaan"] = df["jenis_perusahaan"].fillna("Unknown")
    df["kode_negara"] = df["kode_negara"].fillna("INTL")
    return df


def features_for_upcoming(history: pd.DataFrame, upcoming: pd.DataFrame) -> pd.DataFrame:
    """Fitur untuk jadwal mendatang, memakai seluruh riwayat sampai hari ini."""
    h = history.copy()
    rate_now = h["is_gagal"].mean() if len(h) else 0.1
    up = upcoming.copy()
    up["ts"] = pd.to_datetime(up["net_waktu"], utc=True).dt.tz_localize(None)
    now = pd.Timestamp.utcnow().tz_localize(None)
    ref = up["ts"].where(up["ts"] > now, now)
    out = []
    by_r, by_p = h.groupby("nama_roket"), h.groupby("nama_perusahaan")
    for i, r in up.iterrows():
        hr = by_r.get_group(r["nama_roket"]) if r["nama_roket"] in by_r.groups else h.iloc[0:0]
        hp = by_p.get_group(r["nama_perusahaan"]) if r["nama_perusahaan"] in by_p.groups else h.iloc[0:0]
        n_r, n_p = len(hr), len(hp)
        out.append(dict(
            roket_n_prior=n_r,
            roket_fail_rate_prior=(hr["is_gagal"].sum() + PRIOR_STRENGTH * rate_now) / (n_r + PRIOR_STRENGTH),
            roket_is_maiden=int(n_r == 0), roket_is_early=int(n_r < 3),
            roket_age_years=((ref[i] - hr["ts"].min()).days / 365.25) if n_r else 0.0,
            roket_days_since_last=np.log1p(min(max((ref[i] - hr["ts"].max()).days, 0), 3650)) if n_r else np.log1p(3650),
            prov_n_prior=n_p,
            prov_fail_rate_prior=(hp["is_gagal"].sum() + PRIOR_STRENGTH * rate_now) / (n_p + PRIOR_STRENGTH),
            prov_fail_last10=hp["is_gagal"].tail(10).sum() if n_p else 0,
            prov_days_since_last=np.log1p(min(max((ref[i] - hp["ts"].max()).days, 0), 3650)) if n_p else np.log1p(3650),
            industry_launches_365d=int((h["ts"] >= now - pd.Timedelta(days=365)).sum()),
            tahun=ref[i].year,
            jenis_perusahaan=r.get("jenis_perusahaan") or "Unknown",
            kode_negara=r.get("kode_negara") or "INTL"))
    feat = pd.DataFrame(out)
    # buang kolom yang juga ada di feat supaya X[FEATURES] tidak berisi kolom ganda
    up = up.drop(columns=[c for c in feat.columns if c in up.columns])
    return pd.concat([up.reset_index(drop=True), feat], axis=1)
