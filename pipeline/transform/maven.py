"""Transform arsip internal (staging.stg_maven_raw -> staging.stg_peluncuran_clean).

Aturan:
  TC-01 hapus baris identik         TC-02 perbaiki encoding (ftfy)    TC-03 'n†­' -> 'n°'
  TC-04 koreksi_manual              TC-06 harga kosong tetap NULL
  TV-01 tanggal valid 1957-2040     TV-02 waktu HH:MM:SS / NULL + flag TV-03 harga angka positif
  TV-04 status dikenal              TV-07 kunci sumber unik
  TS-01 harga teks -> numeric       TS-04 Retired -> Inactive         TS-06/07/08 pecah lokasi + ISO alpha-3
  TE-01 kunci MVN-<md5>             TE-02 sumber_data/sumber_harga    TE-03 is_sukses
"""
import hashlib
import re
from datetime import date

import ftfy
import pandas as pd

from .. import db
from ..quality.checks import DQ, quality_gate
from ..runlog import track_batch, write_lineage
from .common import STATUS_ARSIP, Maps, RejectCollector

BROKEN_NO = "â\u0080 Â­"      # 'n†­' setelah dibaca ISO-8859-1 (TC-03)
TEXT_COLS = ["company", "location", "rocket", "mission"]
TIME_RE = re.compile(r"^\d{2}:\d{2}:\d{2}$")
RULES = ["TC-01", "TC-02", "TC-03", "TC-04", "TC-06", "TV-01", "TV-02", "TV-03", "TV-04", "TV-07",
         "TS-01", "TS-04", "TS-06", "TS-07", "TS-08", "TE-01", "TE-02", "TE-03"]


def fix_text(s, kolom, koreksi):
    if s is None:
        return None
    s = s.replace(BROKEN_NO, "°")                     # TC-03
    s = ftfy.fix_text(s)                              # TC-02
    return koreksi.get((kolom, s), s).strip()        # TC-04


def parse_lokasi(loc, negara_map):
    """TS-06/07/08: 'landasan, situs, [wilayah...], negara' -> komponen terpisah + kode ISO."""
    parts = [p.strip() for p in loc.split(",")]
    last = parts[-1]
    if last not in negara_map:
        return None
    kode, extra = negara_map[last]
    body = parts[:-1]
    landasan = situs = wilayah = None
    if extra and len(body) == 1 and re.search(r"(Site|Facility)$", extra):
        landasan, situs = body[0], extra              # 'Launch Plateform, Shahrud Missile Test Site'
        extra = None
    elif len(body) == 1:
        situs = body[0]
    elif len(body) == 2:
        landasan, situs = body
    else:
        landasan, situs, wilayah = body[0], body[1], " ".join(body[2:])
    if extra:
        wilayah = f"{wilayah} {extra}" if wilayah else extra
    return landasan, situs, wilayah, kode


def transform_maven(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "transform_maven", "maven_csv") as b:
        src = db.execute("SELECT source_object FROM staging.stg_maven_raw ORDER BY extracted_at DESC LIMIT 1",
                         fetch=True)
        if not src:
            b.status = "SKIPPED"
            b.note("stg_maven_raw kosong")
            return 0
        src_obj = src[0][0]
        done = db.scalar("""SELECT COUNT(*) FROM meta.lineage WHERE target_table='staging.stg_peluncuran_clean'
                            AND %s = ANY(source_refs)""", (src_obj,))
        if done:
            b.status = "SKIPPED"
            b.note(f"{src_obj} sudah ditransformasi")
            return 0
        raw = db.read_sql("""SELECT row_num, company, location, date, time, rocket, mission, rocket_status,
                                    price, mission_status FROM staging.stg_maven_raw
                             WHERE source_object=%s ORDER BY row_num""", (src_obj,))
        b.rows_read = n_total = len(raw)
        maps = Maps()
        rej = RejectCollector("maven_csv", "stg_peluncuran_clean")
        dq = DQ(b.batch_id, "staging", "stg_maven_raw")

        # profiling awal (data mentah)
        dq.completeness(raw, ["company", "location", "date", "rocket", "mission", "mission_status"])
        dq.check("DQ-02", "time kosong (diterima, flag waktu_diketahui=0)", "completeness",
                 raw.time.isna().sum(), n_total, "INFO", 100)
        dq.check("TC-06", "price kosong (tetap NULL, tidak diimputasi)", "completeness",
                 raw.price.isna().sum(), n_total, "INFO", 100)

        # TC-01 duplikat identik
        cols9 = ["company", "location", "date", "time", "rocket", "mission", "rocket_status", "price",
                 "mission_status"]
        dup_mask = raw.duplicated(subset=cols9, keep="first")
        dq.check("TC-01", "baris identik di semua kolom (dihapus, disimpan satu)", "uniqueness",
                 dup_mask.sum(), n_total, "INFO", 100)
        for r in raw[dup_mask].itertuples():
            rej.add(f"row {r.row_num}", "TC-01", "baris duplikat identik dihapus", r._asdict())
        df = raw[~dup_mask].copy()

        # TC-02..04 perbaikan teks
        before = df[TEXT_COLS].copy()
        for c in TEXT_COLS:
            df[c] = df[c].map(lambda s, c=c: fix_text(s, c, maps.koreksi))
        changed = (before.fillna("") != df[TEXT_COLS].fillna(""))
        for c in TEXT_COLS:
            dq.check("TC-02", f"perbaikan encoding/teks kolom {c}", "accuracy", changed[c].sum(), len(df),
                     "INFO", 100)
        residual = df[TEXT_COLS].apply(lambda s: s.str.contains(r"[\u0080-\u009f†]", na=False)).any(axis=1)
        dq.check("TC-04", "sisa karakter rusak setelah perbaikan", "validity", residual.sum(), len(df), "WARN")

        out, n_reject = [], 0
        for r in df.itertuples(index=False):
            rec = r._asdict()
            ref = f"row {r.row_num}"
            # TV-01 tanggal
            try:
                tgl = date.fromisoformat(r.date)
                if not (date(1957, 1, 1) <= tgl <= date(2040, 12, 31)):
                    raise ValueError("di luar rentang dim_tanggal")
            except (TypeError, ValueError) as e:
                rej.add(ref, "TV-01", f"tanggal tidak valid: {r.date} ({e})", rec); continue
            # TV-02 waktu
            waktu = r.time if (r.time and TIME_RE.match(r.time)) else None
            # TS-01 / TV-03 harga
            harga = None
            if r.price:
                try:
                    harga = round(float(r.price.replace(",", "")), 2)
                    if harga <= 0:
                        raise ValueError("harga <= 0")
                except ValueError as e:
                    rej.add(ref, "TV-03", f"harga tidak valid: {r.price} ({e})", rec); continue
            # TV-04 status
            if r.mission_status not in STATUS_ARSIP:
                rej.add(ref, "TV-04", f"status misi tidak dikenal: {r.mission_status}", rec); continue
            # TS-04 status roket
            st_roket = {"Retired": "Inactive", "Inactive": "Inactive", "Active": "Active"}.get(r.rocket_status)
            # TS-06/07/08 lokasi
            lok = parse_lokasi(r.location, maps.negara)
            if lok is None:
                rej.add(ref, "TS-07", f"negara tidak terpetakan: {r.location}", rec); continue
            landasan, situs, wilayah, kode = lok
            # TS-09: nama situs lama/baru diseragamkan (mis. Cape Canaveral AFS -> SFS) lewat map_lokasi '*'
            if ("*", situs) in maps.lokasi:
                situs = maps.lokasi[("*", situs)][1]
            perusahaan = maps.map_perusahaan(r.company, ref)
            # TE-01 kunci sumber
            key = "MVN-" + hashlib.md5("|".join(str(x or "") for x in
                                                (r.company, r.date, r.time, r.rocket, r.mission)).encode()).hexdigest()
            out.append(dict(
                sumber_id=key, sumber_data="Arsip Internal", tanggal=tgl, waktu=waktu,
                waktu_diketahui=1 if waktu else 0, perusahaan_nama=perusahaan,
                perusahaan_jenis=maps.jenis.get(perusahaan), roket_nama=r.rocket[:100], ll2_config_id=None,
                roket_status=st_roket, landasan=landasan, situs=situs, wilayah=wilayah, kode_negara=kode,
                latitude=None, longitude=None, orbit_singkatan=None, orbit_nama=None,
                status_nama=r.mission_status, nama_misi=(r.mission or "")[:200], jenis_misi=None,
                alasan_gagal=None, harga_musd=harga,
                sumber_harga="Arsip Internal" if harga is not None else None,          # TE-02
                is_sukses=1 if r.mission_status == "Success" else 0,                     # TE-03
                last_updated=None, batch_id=b.batch_id))
        clean = pd.DataFrame(out)

        # validasi hasil clean
        dq.table = "stg_peluncuran_clean"
        dq.uniqueness(clean, ["sumber_id"])
        dq.in_set(clean.status_nama, STATUS_ARSIP, "status misi termasuk 4 kategori arsip", "TV-04")
        dq.in_set(clean.roket_status.dropna(), {"Active", "Inactive"}, "status roket Active/Inactive", "TS-04")
        dq.check("TS-04", "status roket tidak terpetakan", "validity", clean.roket_status.isna().sum(),
                 len(clean), "WARN")
        dq.in_range(clean.harga_musd, 0.01, 10000, "harga_musd positif dan wajar (<= 10.000 juta USD)", "TV-03")
        dq.check("TS-07", "kode negara kosong (perairan internasional)", "completeness",
                 clean.kode_negara.isna().sum(), len(clean), "INFO", 100)
        dq.check("DQ-03", "konsistensi is_sukses vs status", "consistency",
                 ((clean.is_sukses == 1) != (clean.status_nama == "Success")).sum(), len(clean))
        dq.check("TC-01", "jumlah baris clean = mentah - duplikat - reject", "consistency",
                 abs(len(clean) - (n_total - dup_mask.sum() - (rej.count() - rej.count("TC-01")))), n_total)

        n_reject = rej.count() - rej.count("TC-01")      # duplikat bukan data invalid
        b.rows_rejected = rej.write(b.batch_id)
        dq.write()
        quality_gate(b.batch_id, "maven_csv", n_total, n_reject, dq)

        # load ke staging clean (upsert)
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_peluncuran_clean WHERE sumber_data='Arsip Internal' "
                        "AND sumber_id <> ALL(%s)", (list(clean.sumber_id),))
            upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in clean.columns if c != "sumber_id")
            b.rows_written = db.insert_df(cur, "staging.stg_peluncuran_clean", clean,
                                          on_conflict=f"ON CONFLICT (sumber_id) DO UPDATE SET {upd}, transformed_at=now()")
        maps.flush_review_queue(b.batch_id)
        write_lineage(b.batch_id, "staging.stg_peluncuran_clean", "staging", [src_obj], RULES, b.rows_written)
        b.note(f"{n_total} mentah -> {b.rows_written} clean, {dup_mask.sum()} duplikat, {n_reject} reject")
        return b.rows_written
