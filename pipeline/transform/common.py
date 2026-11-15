"""Utilitas transformasi bersama: tabel pemetaan, normalisasi nama, review queue, quarantine."""

import re

import pandas as pd

from .. import config, db
from ..runlog import log

# TS-05: pemetaan status LL2 -> kategori arsip
STATUS_LL2_TO_ARSIP = {
    "Launch Successful": "Success",
    "Launch Failure": "Failure",
    "Launch was a Partial Failure": "Partial Failure",
}
STATUS_ARSIP = {"Success", "Failure", "Partial Failure", "Prelaunch Failure"}
STATUS_LL2_NONFINAL = {"Go for Launch", "To Be Determined", "To Be Confirmed", "On Hold",
                       "Launch in Flight", "Payload Deployed"}
STATUS_LL2_ALL = set(STATUS_LL2_TO_ARSIP) | STATUS_LL2_NONFINAL   # TV-04: 9 status LL2
PRESISI_WAKTU_TEPAT = {None, "Second", "Minute", "Hour"}          # waktu_diketahui = 1


def norm(s) -> str:
    """Kunci pencocokan nama: huruf kecil, tanpa spasi/tanda baca."""
    return re.sub(r"[^a-z0-9]", "", str(s).lower()) if s is not None else ""


# seed mapping
def seed_mappings():
    """Muat ulang tabel pemetaan dari folder mappings/ (sumber kebenaran di Git)."""
    d = config.MAPPING_DIR
    specs = {
        "meta.map_perusahaan": ("map_perusahaan.csv", ["nama_sumber", "nama_baku", "jenis_perusahaan"]),
        "meta.map_roket": ("map_roket.csv", ["nama_sumber", "nama_baku"]),
        "meta.map_lokasi": ("map_lokasi.csv", ["pad_sumber", "situs_sumber", "landasan_baku", "situs_baku"]),
        "meta.map_orbit": ("map_orbit.csv", ["singkatan", "kelompok_orbit"]),
        "meta.map_negara": ("map_negara.csv", ["nama_sumber", "kode_negara", "wilayah_tambahan"]),
        "meta.koreksi_manual": ("koreksi_manual.csv", ["kolom", "nilai_asli", "nilai_koreksi", "alasan"]),
    }
    counts = {}
    with db.transaction() as conn, conn.cursor() as cur:
        for table, (fname, cols) in specs.items():
            df = pd.read_csv(d / fname, dtype=str, keep_default_na=False, encoding="utf-8")[cols]
            df = df.replace({"": None})
            if table == "meta.koreksi_manual":   # nilai rusak disimpan dalam notasi \uXXXX
                df["nilai_asli"] = df["nilai_asli"].map(
                    lambda s: re.sub(r"\\u([0-9a-fA-F]{4})", lambda m: chr(int(m.group(1), 16)), s))
            cur.execute(f"TRUNCATE {table}")
            counts[table] = db.insert_df(cur, table, df.drop_duplicates(subset=cols[:2] if table in
                                         ("meta.map_lokasi", "meta.koreksi_manual") else cols[:1]))
        # TI-05: nama di review queue yang sudah ditambahkan ke file mapping ditandai MAPPED
        cur.execute("""UPDATE meta.review_queue SET status = 'MAPPED'
                       WHERE status = 'PENDING' AND (
                         (entity_type = 'perusahaan' AND raw_value IN (SELECT nama_sumber FROM meta.map_perusahaan))
                      OR (entity_type = 'roket' AND raw_value IN (SELECT nama_sumber FROM meta.map_roket))
                      OR (entity_type = 'lokasi' AND raw_value IN (SELECT situs_sumber FROM meta.map_lokasi)))""")
        counts["review_queue_mapped"] = cur.rowcount
    log.info("seed mapping: %s", counts)
    return counts


class Maps:
    """Snapshot tabel pemetaan di memori untuk satu proses transform."""

    def __init__(self):
        q = db.read_sql
        mp = q("SELECT nama_sumber, nama_baku, jenis_perusahaan FROM meta.map_perusahaan")
        self.perusahaan = dict(zip(mp.nama_sumber, mp.nama_baku))
        self.jenis = dict(zip(mp.nama_baku, mp.jenis_perusahaan))
        self.perusahaan_norm = {norm(k): v for k, v in self.perusahaan.items()}
        mr = q("SELECT nama_sumber, nama_baku FROM meta.map_roket")
        self.roket = dict(zip(mr.nama_sumber, mr.nama_baku))
        ml = q("SELECT pad_sumber, situs_sumber, landasan_baku, situs_baku FROM meta.map_lokasi")
        self.lokasi = {(r.pad_sumber, r.situs_sumber): (r.landasan_baku, r.situs_baku) for r in ml.itertuples()}
        mo = q("SELECT singkatan, kelompok_orbit FROM meta.map_orbit")
        self.orbit = dict(zip(mo.singkatan, mo.kelompok_orbit))
        mn = q("SELECT nama_sumber, kode_negara, wilayah_tambahan FROM meta.map_negara")
        self.negara = {r.nama_sumber: (r.kode_negara, r.wilayah_tambahan) for r in mn.itertuples()}
        mk = q("SELECT kolom, nilai_asli, nilai_koreksi FROM meta.koreksi_manual")
        self.koreksi = {(r.kolom, r.nilai_asli): r.nilai_koreksi for r in mk.itertuples()}
        # nama roket & situs baku dari arsip internal (dipakai untuk auto-match nama LL2)
        arsip = q("SELECT DISTINCT roket_nama FROM staging.stg_peluncuran_clean WHERE sumber_data='Arsip Internal'")
        self.roket_arsip_norm = {norm(n): n for n in arsip.roket_nama}
        situs = q("SELECT DISTINCT situs FROM staging.stg_peluncuran_clean WHERE sumber_data='Arsip Internal'")
        self.situs_arsip_norm = {norm(n): n for n in situs.situs if n}
        self.unmapped = []          # (entity_type, raw_value, sample_ref)

    # TI-02 / TI-06
    def map_perusahaan(self, name, ref=None):
        if not name:
            return "Unknown"
        if name in self.perusahaan:
            return self.perusahaan[name]
        if norm(name) in self.perusahaan_norm:
            return self.perusahaan_norm[norm(name)]
        self.unmapped.append(("perusahaan", name, ref))
        return name

    # TI-03
    def map_roket(self, full_name, short_name=None, ref=None):
        if full_name in self.roket:
            return self.roket[full_name]
        for cand in (full_name, short_name):
            if cand and norm(cand) in self.roket_arsip_norm:
                return self.roket_arsip_norm[norm(cand)]
        self.unmapped.append(("roket", full_name, ref))
        return full_name

    # TS-09 / TI-04
    def map_lokasi(self, pad_name, location_name, ref=None):
        situs_raw = (location_name or "Unknown").split(",")[0].strip()
        if (pad_name, situs_raw) in self.lokasi:                 # pemetaan spesifik pad + situs
            return self.lokasi[(pad_name, situs_raw)]
        landasan = normalize_pad(pad_name)
        if ("*", situs_raw) in self.lokasi:                      # pemetaan situs untuk semua pad
            lb, sb = self.lokasi[("*", situs_raw)]
            return (lb or landasan), sb
        situs = self.situs_arsip_norm.get(norm(situs_raw))
        if situs is None:
            self.unmapped.append(("lokasi", situs_raw, ref))
            situs = situs_raw
        return landasan, situs

    def flush_review_queue(self, batch_id):
        """TI-05: nama yang belum terpetakan masuk daftar tinjauan data steward."""
        seen = {}
        for et, raw, ref in self.unmapped:
            seen.setdefault((et, raw), ref)
        if not seen:
            return 0
        with db.transaction() as conn, conn.cursor() as cur:
            rows = [(et, raw, ref, batch_id) for (et, raw), ref in seen.items()]
            db.psycopg2.extras.execute_values(
                cur, """INSERT INTO meta.review_queue (entity_type, raw_value, sample_ref, first_batch)
                        VALUES %s ON CONFLICT (entity_type, raw_value) DO NOTHING""", rows)
        return len(seen)


_PAD_RULES = [
    (r"^Space Launch Complex\s+", "SLC-"),
    (r"^Launch Complex\s+", "LC-"),
    (r"^Rocket Lab Launch Complex\s+", "Rocket Lab LC-"),
    (r"^Launch Area\s+", "LA-"),
    (r"^Launch Pad\s+", "LP-"),
]


def normalize_pad(pad_name):
    """'Space Launch Complex 4E' -> 'SLC-4E', 'Launch Complex 39A' -> 'LC-39A' (konvensi arsip)."""
    if not pad_name:
        return None
    s = pad_name.strip()
    for pat, rep in _PAD_RULES:
        s = re.sub(pat, rep, s)
    return s[:100]


class RejectCollector:
    """Mengumpulkan baris gagal validasi -> staging.stg_reject (quarantine)."""

    def __init__(self, source, target_table):
        self.source, self.target, self.rows = source, target_table, []

    def add(self, record_ref, rule_id, reason, payload):
        self.rows.append((self.source, self.target, str(record_ref), rule_id, reason, payload))

    def count(self, rule_prefix=None):
        return sum(1 for r in self.rows if rule_prefix is None or r[3].startswith(rule_prefix))

    def write(self, batch_id):
        if not self.rows:
            return 0
        with db.transaction() as conn, conn.cursor() as cur:
            db.psycopg2.extras.execute_values(
                cur, """INSERT INTO staging.stg_reject (batch_id, source, target_table, record_ref, rule_id,
                        reason, payload) VALUES %s""",
                [(batch_id, s, t, ref, rule, reason, db.json.dumps(p, default=str, ensure_ascii=False))
                 for s, t, ref, rule, reason, p in self.rows])
        return len(self.rows)
