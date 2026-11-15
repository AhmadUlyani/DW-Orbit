"""Transform Launch Library 2 (staging raw jsonb -> staging clean).

  transform_configs  : stg_ll2_config_raw  -> stg_roket_clean      (TS-02, TS-04, TV-09, TI-03, TI-06, TE-06)
  transform_launches : stg_ll2_launch_raw  -> stg_peluncuran_clean (TC-05, TV-01, TV-04..07, TS-02/03/05/09/10,
                       (endpoint previous)                         TI-01..05, TE-02, TE-03)
  transform_upcoming : stg_ll2_launch_raw  -> stg_jadwal_clean     (TE-04 hari_menuju, TE-05 geser_jam)
                       (endpoint upcoming; semua snapshot tertunda, urut dari tanggal paling lama)
"""
from datetime import date, datetime, timezone

import pandas as pd

from .. import config, db
from ..ll2_client import WatermarkStore
from ..quality.checks import DQ, QualityGateBlocked, quality_gate
from ..runlog import log, track_batch, write_lineage
from .common import (PRESISI_WAKTU_TEPAT, STATUS_LL2_ALL, STATUS_LL2_TO_ARSIP, Maps, RejectCollector)

CUTOFF = datetime.fromisoformat(config.ARCHIVE_CUTOFF_UTC.replace("Z", "+00:00"))
JADWAL_COLS = ["ll2_launch_id", "snapshot_date", "net_waktu", "window_start", "window_end", "presisi_jadwal",
               "status_nama", "perusahaan_nama", "perusahaan_jenis", "roket_nama", "ll2_config_id", "landasan",
               "situs", "wilayah", "kode_negara", "latitude", "longitude", "orbit_singkatan", "orbit_nama",
               "nama_misi", "jenis_misi", "hari_menuju_peluncuran", "geser_jam", "is_bergeser", "batch_id"]
ROKET_COLS = ["ll2_config_id", "snapshot_date", "roket_nama", "produsen_nama", "produsen_jenis", "status_roket",
              "perkiraan_biaya_musd", "total_peluncuran", "peluncuran_sukses", "peluncuran_gagal", "sukses_beruntun",
              "peluncuran_terjadwal", "is_full_snapshot", "batch_id"]
DMIN, DMAX = date(1957, 1, 1), date(2040, 12, 31)


def _g(d, *path):
    for p in path:
        if not isinstance(d, dict):
            return None
        d = d.get(p)
    return d


def _ts(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")) if s else None


def _num(v):
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


def _latest_config_info():
    """Status aktif & perkiraan biaya terbaru per id konfigurasi (dari stg_roket_clean)."""
    df = db.read_sql("""SELECT DISTINCT ON (ll2_config_id) ll2_config_id, status_roket, perkiraan_biaya_musd
                        FROM staging.stg_roket_clean ORDER BY ll2_config_id, snapshot_date DESC""")
    return {int(r.ll2_config_id): (r.status_roket, r.perkiraan_biaya_musd) for r in df.itertuples()}


def _launch_common(it, maps: Maps, rej: RejectCollector, ref):
    """Atribut bersama data peluncuran & jadwal. Return dict atau None jika di-reject."""
    status = _g(it, "status", "name")
    if status not in STATUS_LL2_ALL:                                               # TV-04
        rej.add(ref, "TV-04", f"status LL2 tidak dikenal: {status}", it); return None
    net = _ts(it.get("net"))
    if net is None or not (DMIN <= net.date() <= DMAX):                            # TV-01
        rej.add(ref, "TV-01", f"net di luar rentang dim_tanggal: {it.get('net')}", it); return None
    lat, lon = _num(_g(it, "pad", "latitude")), _num(_g(it, "pad", "longitude"))
    if (lat is not None and not -90 <= lat <= 90) or (lon is not None and not -180 <= lon <= 180):  # TV-06
        rej.add(ref, "TV-06", f"koordinat tidak valid: {lat},{lon}", it); return None
    lsp = _g(it, "launch_service_provider", "name")
    perusahaan = maps.map_perusahaan(lsp, ref)                                     # TI-02
    cfg_id = _g(it, "rocket", "configuration", "id")
    roket = maps.map_roket(_g(it, "rocket", "configuration", "full_name"),          # TI-03
                           _g(it, "rocket", "configuration", "name"), ref)
    landasan, situs = maps.map_lokasi(_g(it, "pad", "name"), _g(it, "pad", "location", "name"), ref)  # TS-09/TI-04
    kode = _g(it, "pad", "country", "alpha_3_code") or _g(it, "pad", "location", "country", "alpha_3_code")
    mission = it.get("mission") or {}
    nama_misi = mission.get("name") or (it.get("name", "").split(" | ", 1)[-1])
    return dict(
        status_raw=status, net=net, presisi=_g(it, "net_precision", "name"),
        perusahaan_nama=perusahaan[:100],
        perusahaan_jenis=maps.jenis.get(perusahaan) or _g(it, "launch_service_provider", "type", "name"),
        roket_nama=(roket or "Unknown")[:100], ll2_config_id=int(cfg_id) if cfg_id is not None else None,
        landasan=landasan, situs=(situs or "Unknown")[:100], wilayah=None,
        kode_negara=kode[:3] if kode else None, latitude=lat, longitude=lon,
        orbit_singkatan=_g(mission, "orbit", "abbrev") or None, orbit_nama=_g(mission, "orbit", "name"),
        nama_misi=(nama_misi or "")[:200], jenis_misi=(mission.get("type") or None),
        last_updated=_ts(it.get("last_updated")))


def transform_configs(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "transform_ll2_configs", "ll2_api") as b:
        wm_name = "transform_ll2_config_raw_id"
        wm = int(WatermarkStore.get(wm_name, 0))
        raw = db.read_sql("""SELECT raw_id, ll2_config_id, snapshot_date, payload, source_object
                             FROM staging.stg_ll2_config_raw WHERE raw_id > %s ORDER BY raw_id""", (wm,))
        if raw.empty:
            b.status = "SKIPPED"; b.note("tidak ada raw konfigurasi baru"); return 0
        b.rows_read = len(raw)
        raw = raw.drop_duplicates(subset=["ll2_config_id", "snapshot_date"], keep="last")   # TC-05
        maps = Maps()
        rej = RejectCollector("ll2_api", "stg_roket_clean")
        out, cost_invalid = [], 0
        for r in raw.itertuples():
            it, ref = r.payload, f"config {r.ll2_config_id}"
            stats = {k: it.get(k) for k in ("total_launch_count", "successful_launches", "failed_launches",
                                            "consecutive_successful_launches", "pending_launches")}
            if any(not isinstance(v, int) or v < 0 for v in stats.values()) or \
                    stats["successful_launches"] + stats["failed_launches"] > stats["total_launch_count"]:
                rej.add(ref, "TV-09", f"statistik peluncuran tidak konsisten: {stats}", it); continue  # TV-09
            cost = _num(it.get("launch_cost"))
            if it.get("launch_cost") is not None and (cost is None or cost <= 0):                    # TV-03
                cost_invalid += 1; cost = None
            produsen = maps.map_perusahaan(_g(it, "manufacturer", "name"), ref)                        # TI-06
            out.append(dict(
                ll2_config_id=int(it["id"]), snapshot_date=r.snapshot_date,                          # TE-06
                roket_nama=maps.map_roket(it.get("full_name"), it.get("name"), ref)[:100],            # TI-03
                produsen_nama=produsen[:100],
                produsen_jenis=maps.jenis.get(produsen) or _g(it, "manufacturer", "type", "name"),
                status_roket="Active" if it.get("active") else "Inactive",                           # TS-04
                perkiraan_biaya_musd=round(cost / 1_000_000, 2) if cost else None,                    # TS-02
                total_peluncuran=stats["total_launch_count"], peluncuran_sukses=stats["successful_launches"],
                peluncuran_gagal=stats["failed_launches"],
                sukses_beruntun=stats["consecutive_successful_launches"],
                peluncuran_terjadwal=stats["pending_launches"],
                is_full_snapshot="dataset=launcher_configurations/" in r.source_object,
                batch_id=b.batch_id))
        clean = pd.DataFrame(out, columns=ROKET_COLS)      # kolom eksplisit: tetap valid bila semua baris ditolak
        dq = DQ(b.batch_id, "staging", "stg_roket_clean")
        dq.uniqueness(clean, ["ll2_config_id", "snapshot_date"])
        dq.completeness(clean, ["roket_nama", "status_roket"])
        dq.check("TV-03", "launch_cost bukan angka positif (disimpan NULL)", "validity", cost_invalid, len(raw), "WARN")
        dq.check("TC-06", "perkiraan biaya kosong (tetap NULL)", "completeness",
                 clean.perkiraan_biaya_musd.isna().sum(), len(clean), "INFO", 100)
        dq.check("TV-09", "statistik peluncuran roket konsisten", "consistency", rej.count("TV-09"), len(raw))
        b.rows_rejected = rej.write(b.batch_id)
        dq.write()
        quality_gate(b.batch_id, "ll2_configs", len(raw), rej.count(), dq)
        with db.transaction() as conn, conn.cursor() as cur:
            upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in clean.columns if c not in ("ll2_config_id", "snapshot_date"))
            b.rows_written = db.insert_df(cur, "staging.stg_roket_clean", clean,
                                          on_conflict=f"ON CONFLICT (ll2_config_id, snapshot_date) DO UPDATE SET {upd}")
        maps.flush_review_queue(b.batch_id)
        WatermarkStore.set(wm_name, int(raw.raw_id.max()))
        write_lineage(b.batch_id, "staging.stg_roket_clean", "staging", sorted(set(raw.source_object)),
                      ["TC-05", "TV-03", "TV-09", "TS-02", "TS-04", "TI-03", "TI-06", "TE-06"], b.rows_written)
        return b.rows_written


def transform_launches(pipeline="orbit_daily_etl"):
    with track_batch(pipeline, "transform_ll2_launches", "ll2_api") as b:
        wm_name = "transform_ll2_previous_raw_id"
        wm = int(WatermarkStore.get(wm_name, 0))
        raw = db.read_sql("""SELECT raw_id, ll2_launch_id::text AS id, last_updated, payload, source_object
                             FROM staging.stg_ll2_launch_raw WHERE endpoint='previous' AND raw_id > %s
                             ORDER BY raw_id""", (wm,))
        if raw.empty:
            b.status = "SKIPPED"; b.note("tidak ada raw peluncuran baru"); return 0
        b.rows_read = n_raw = len(raw)
        # TC-05: id ganda antar halaman/batch -> simpan versi last_updated terbaru
        raw = raw.sort_values(["last_updated", "raw_id"]).drop_duplicates("id", keep="last")
        n_dupe = n_raw - len(raw)
        maps, cfg = Maps(), _latest_config_info()
        rej = RejectCollector("ll2_api", "stg_peluncuran_clean")
        out, pending = [], 0
        for r in raw.itertuples():
            it, ref = r.payload, r.id
            c = _launch_common(it, maps, rej, ref)
            if c is None:
                continue
            if c["net"] <= CUTOFF:                                                  # TI-01
                rej.add(ref, "TI-01", f"net {c['net']} tumpang tindih dengan arsip internal", it); continue
            if c["status_raw"] not in STATUS_LL2_TO_ARSIP:                          # TV-05
                pending += 1; continue
            status = STATUS_LL2_TO_ARSIP[c["status_raw"]]                           # TS-05
            st_roket, biaya = cfg.get(c["ll2_config_id"], (None, None))
            out.append(dict(
                sumber_id=r.id, sumber_data="Launch Library 2", tanggal=c["net"].date(),       # TS-03
                waktu=c["net"].time().replace(microsecond=0),
                waktu_diketahui=1 if c["presisi"] in PRESISI_WAKTU_TEPAT else 0,
                perusahaan_nama=c["perusahaan_nama"], perusahaan_jenis=c["perusahaan_jenis"],
                roket_nama=c["roket_nama"], ll2_config_id=c["ll2_config_id"], roket_status=st_roket,
                landasan=c["landasan"], situs=c["situs"], wilayah=None, kode_negara=c["kode_negara"],
                latitude=c["latitude"], longitude=c["longitude"],
                orbit_singkatan=c["orbit_singkatan"], orbit_nama=c["orbit_nama"],             # TS-10
                status_nama=status, nama_misi=c["nama_misi"], jenis_misi=c["jenis_misi"],
                alasan_gagal=(it.get("failreason") or None),
                harga_musd=biaya, sumber_harga="Launch Library 2" if biaya is not None else None,  # TS-02/TE-02
                is_sukses=1 if status == "Success" else 0,                                    # TE-03
                last_updated=c["last_updated"], batch_id=b.batch_id))
        clean = pd.DataFrame(out)
        dq = DQ(b.batch_id, "staging", "stg_peluncuran_clean")
        dq.check("TC-05", "id ganda antar halaman (disimpan versi terbaru)", "uniqueness", n_dupe, n_raw, "INFO", 100)
        dq.check("TV-05", "status belum final (menunggu di staging)", "timeliness", pending, len(raw), "INFO", 100)
        if not clean.empty:
            dq.uniqueness(clean, ["sumber_id"])
            dq.completeness(clean, ["tanggal", "perusahaan_nama", "roket_nama", "situs", "status_nama"])
            dq.completeness(clean, ["kode_negara", "latitude", "orbit_singkatan"], "DQ-04", "WARN")
            dq.check("DQ-03", "konsistensi is_sukses vs status", "consistency",
                     ((clean.is_sukses == 1) != (clean.status_nama == "Success")).sum(), len(clean))
            dq.check("TI-05", "nama belum terpetakan -> review queue", "consistency",
                     len({(e, v) for e, v, _ in maps.unmapped}), len(clean), "INFO", 100)
        b.rows_rejected = rej.write(b.batch_id)
        dq.write()
        quality_gate(b.batch_id, "ll2_launches", len(raw), rej.count(), dq)
        if not clean.empty:
            with db.transaction() as conn, conn.cursor() as cur:
                upd = ", ".join(f"{c}=EXCLUDED.{c}" for c in clean.columns if c != "sumber_id")
                b.rows_written = db.insert_df(
                    cur, "staging.stg_peluncuran_clean", clean,
                    on_conflict=f"""ON CONFLICT (sumber_id) DO UPDATE SET {upd}, transformed_at=now()
                                    WHERE staging.stg_peluncuran_clean.last_updated IS NULL
                                       OR EXCLUDED.last_updated >= staging.stg_peluncuran_clean.last_updated""")
        maps.flush_review_queue(b.batch_id)
        WatermarkStore.set(wm_name, int(db.scalar(
            "SELECT COALESCE(MAX(raw_id),0) FROM staging.stg_ll2_launch_raw WHERE endpoint='previous'")))
        write_lineage(b.batch_id, "staging.stg_peluncuran_clean", "staging", sorted(set(raw.source_object)),
                      ["TC-05", "TV-01", "TV-04", "TV-05", "TV-06", "TS-02", "TS-03", "TS-05", "TS-09", "TS-10",
                       "TI-01", "TI-02", "TI-03", "TI-04", "TI-05", "TE-02", "TE-03"], b.rows_written)
        b.note(f"{n_raw} raw, {n_dupe} duplikat, {pending} belum final, {b.rows_rejected} reject, "
               f"{b.rows_written} upsert")
        return b.rows_written


SQL_PENDING_SNAPSHOTS = """
WITH r AS (SELECT snapshot_date, MAX(extracted_at) AS raw_at FROM staging.stg_ll2_launch_raw
           WHERE endpoint = 'upcoming' GROUP BY snapshot_date),
     c AS (SELECT snapshot_date, MAX(transformed_at) AS clean_at FROM staging.stg_jadwal_clean GROUP BY snapshot_date)
SELECT r.snapshot_date FROM r LEFT JOIN c USING (snapshot_date)
WHERE c.clean_at IS NULL OR r.raw_at > c.clean_at
ORDER BY r.snapshot_date
"""


def pending_snapshots():
    """Snapshot jadwal di staging raw yang belum (atau belum lengkap) ditransformasi."""
    return [r[0] for r in db.execute(SQL_PENDING_SNAPSHOTS, fetch=True)]


def transform_upcoming(pipeline="orbit_daily_etl"):
    """Transform semua snapshot jadwal yang tertunda, urut dari tanggal paling lama.

    geser_jam (TE-05) dihitung terhadap snapshot sebelumnya, jadi kalau snapshot lama baru masuk
    belakangan, semua snapshot sesudahnya ikut ditransformasi ulang. Snapshot yang diblok quality gate
    tidak menghentikan snapshot lain; task tetap gagal di akhir dan snapshot itu dicoba lagi di run
    berikutnya.
    """
    pending = pending_snapshots()
    if not pending:
        log.info("tidak ada snapshot jadwal yang tertunda")
        return 0
    all_dates = [r[0] for r in db.execute("""SELECT DISTINCT snapshot_date FROM staging.stg_ll2_launch_raw
                                              WHERE endpoint='upcoming' AND snapshot_date >= %s ORDER BY 1""",
                                           (pending[0],), fetch=True)]
    log.info("snapshot tertunda %s -> diproses berurutan (termasuk snapshot sesudahnya): %s", pending, all_dates)
    total, blocked = 0, []
    for d in all_dates:
        try:
            total += transform_snapshot(d, pipeline=pipeline)
        except QualityGateBlocked as exc:
            blocked.append(str(d))
            log.error("snapshot %s diblok quality gate: %s", d, exc)
    if blocked:
        raise QualityGateBlocked(f"snapshot jadwal diblok quality gate: {blocked}; snapshot lain tetap diproses")
    return total


def transform_snapshot(snapshot_date: date, pipeline="orbit_daily_etl"):
    """Transform satu snapshot jadwal (staging raw upcoming -> stg_jadwal_clean)."""
    with track_batch(pipeline, "transform_ll2_upcoming", "ll2_api") as b:
        raw = db.read_sql("""SELECT raw_id, ll2_launch_id::text AS id, last_updated, payload, source_object
                             FROM staging.stg_ll2_launch_raw WHERE endpoint='upcoming' AND snapshot_date=%s""",
                          (snapshot_date,))
        if raw.empty:
            b.status = "SKIPPED"; b.note(f"tidak ada snapshot upcoming {snapshot_date}"); return 0
        b.rows_read = n_raw = len(raw)
        raw = raw.sort_values(["last_updated", "raw_id"]).drop_duplicates("id", keep="last")   # TC-05
        # TE-05: net pada snapshot sebelumnya untuk id yang sama
        prev = db.read_sql("""SELECT DISTINCT ON (ll2_launch_id) ll2_launch_id::text AS id, net_waktu
                              FROM staging.stg_jadwal_clean WHERE snapshot_date < %s
                              ORDER BY ll2_launch_id, snapshot_date DESC""", (snapshot_date,))
        prev_net = dict(zip(prev.id, prev.net_waktu))
        maps = Maps()
        rej = RejectCollector("ll2_api", "stg_jadwal_clean")
        out = []
        for r in raw.itertuples():
            it, ref = r.payload, r.id
            c = _launch_common(it, maps, rej, ref)
            if c is None:
                continue
            p = prev_net.get(r.id)
            geser = round((c["net"] - p).total_seconds() / 3600, 2) if p is not None else None
            out.append(dict(
                ll2_launch_id=r.id, snapshot_date=snapshot_date, net_waktu=c["net"],
                window_start=_ts(it.get("window_start")), window_end=_ts(it.get("window_end")),
                presisi_jadwal=c["presisi"], status_nama=STATUS_LL2_TO_ARSIP.get(c["status_raw"], c["status_raw"]),
                perusahaan_nama=c["perusahaan_nama"], perusahaan_jenis=c["perusahaan_jenis"],
                roket_nama=c["roket_nama"], ll2_config_id=c["ll2_config_id"], landasan=c["landasan"],
                situs=c["situs"], wilayah=None, kode_negara=c["kode_negara"], latitude=c["latitude"],
                longitude=c["longitude"], orbit_singkatan=c["orbit_singkatan"], orbit_nama=c["orbit_nama"],
                nama_misi=c["nama_misi"], jenis_misi=c["jenis_misi"],
                hari_menuju_peluncuran=(c["net"].date() - snapshot_date).days,                 # TE-04
                geser_jam=geser, is_bergeser=None if geser is None else int(geser != 0),       # TE-05
                batch_id=b.batch_id))
        clean = pd.DataFrame(out, columns=JADWAL_COLS)     # kolom eksplisit: tetap valid bila semua baris ditolak
        dq = DQ(b.batch_id, "staging", "stg_jadwal_clean")
        dq.check("TC-05", "id ganda antar halaman API (paging bergeser, disimpan versi terbaru)", "uniqueness",
                 n_raw - len(raw), n_raw, "INFO", 100)
        api_count = db.scalar("SELECT total_count FROM meta.ingest_checkpoint WHERE job=%s",
                              (f"ll2_upcoming@{snapshot_date}",)) or len(raw)
        dq.check("DQ-06", f"kelengkapan snapshot: {len(raw)} id unik vs count API {api_count}", "completeness",
                 max(api_count - len(raw), 0), api_count, "WARN", 10)
        dq.uniqueness(clean, ["ll2_launch_id", "snapshot_date"])
        dq.completeness(clean, ["net_waktu", "perusahaan_nama", "roket_nama", "status_nama"])
        dq.check("DQ-05", "jadwal dengan net sudah lewat dari tanggal snapshot", "timeliness",
                 (clean.hari_menuju_peluncuran < -1).sum(), len(clean), "WARN", 5)
        dq.check("TE-05", "jadwal tanpa snapshot pembanding (geser_jam NULL)", "completeness",
                 clean.geser_jam.isna().sum(), len(clean), "INFO", 100)
        b.rows_rejected = rej.write(b.batch_id)
        dq.write()
        quality_gate(b.batch_id, "ll2_upcoming", len(raw), rej.count(), dq)     # pembagi = id unik
        with db.transaction() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_jadwal_clean WHERE snapshot_date=%s", (snapshot_date,))
            b.rows_written = db.insert_df(cur, "staging.stg_jadwal_clean", clean)
        maps.flush_review_queue(b.batch_id)
        write_lineage(b.batch_id, "staging.stg_jadwal_clean", "staging", sorted(set(raw.source_object)),
                      ["TC-05", "TV-01", "TV-04", "TV-06", "TS-03", "TS-09", "TS-10", "TI-02", "TI-03",
                       "TI-04", "TE-04", "TE-05"], b.rows_written)
        b.note(f"snapshot {snapshot_date}: {b.rows_written} jadwal, {int(pd.to_numeric(clean.is_bergeser).fillna(0).sum())} bergeser")
        return b.rows_written
