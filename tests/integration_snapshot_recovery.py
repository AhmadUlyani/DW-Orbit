"""Uji integrasi: pemulihan snapshot jadwal yang tertunda + guard sekali per hari.

Jalankan (stack Docker harus menyala):
    docker compose run --rm --no-deps airflow-scheduler python -m tests.integration_snapshot_recovery

Data uji = salinan snapshot asli terbaru dengan tanggal mundur 1-2 hari (batch_id -999, source_object 'TEST'),
dan dibersihkan di akhir, termasuk saat uji gagal. Tidak memanggil API dan tidak menyentuh schema dw.

Skenario A  snapshot D-1 sudah ter-ingest tetapi transform-nya gagal, snapshot D sudah ditransformasi.
            -> transform_upcoming() harus memproses D-1 lalu D (berurutan), dan geser_jam D dihitung ulang
               terhadap D-1.
Skenario B  snapshot D-1 diblok quality gate (semua status tidak dikenal).
            -> snapshot D tetap diproses (geser_jam terhadap D-2), task berakhir QualityGateBlocked,
               D-1 tetap tertunda untuk run berikutnya.
Skenario C  guard_once_per_day: True sebelum ETL sukses hari ini, False sesudahnya.
"""
from datetime import timedelta

from pipeline import db
from pipeline.ops.guard import not_yet_ran_today
from pipeline.quality.checks import QualityGateBlocked
from pipeline.transform import ll2

TEST_BATCH = -999
SHIFT_IDS = 5          # jumlah jadwal yang dibuat "bergeser" pada snapshot uji


def _insert_copy(src_date, new_date, net_shift_hours=0, bad_status=False):
    """Salin snapshot src_date sebagai new_date. Untuk SHIFT_IDS id pertama, net dimundurkan."""
    db.execute(f"""
        INSERT INTO staging.stg_ll2_launch_raw (batch_id, endpoint, ll2_launch_id, last_updated, snapshot_date,
                                                payload, source_object)
        SELECT {TEST_BATCH}, 'upcoming', r.ll2_launch_id, r.last_updated, %(nd)s,
               CASE WHEN %(bad)s THEN jsonb_set(r.payload, '{{status,name}}', '"STATUS-TIDAK-DIKENAL"')
                    WHEN r.ll2_launch_id IN (SELECT ll2_launch_id FROM staging.stg_ll2_launch_raw
                                             WHERE endpoint='upcoming' AND snapshot_date=%(sd)s
                                             ORDER BY ll2_launch_id LIMIT {SHIFT_IDS})
                    THEN jsonb_set(r.payload, '{{net}}', to_jsonb(to_char(((r.payload->>'net')::timestamptz
                         - make_interval(hours => %(h)s)) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')))
                    ELSE r.payload END,
               'TEST'
        FROM (SELECT DISTINCT ON (ll2_launch_id) * FROM staging.stg_ll2_launch_raw
              WHERE endpoint='upcoming' AND snapshot_date=%(sd)s ORDER BY ll2_launch_id, raw_id DESC) r""",
               {"sd": src_date, "nd": new_date, "h": net_shift_hours, "bad": bad_status})


def _cleanup(d0, dates):
    db.execute("DELETE FROM staging.stg_ll2_launch_raw WHERE batch_id=%s", (TEST_BATCH,))
    db.execute("DELETE FROM staging.stg_jadwal_clean WHERE snapshot_date = ANY(%s)", (dates,))
    db.execute("DELETE FROM staging.stg_reject WHERE source='ll2_api' AND batch_id IN "
               "(SELECT batch_id FROM meta.etl_batch WHERE pipeline='test_snapshot')")
    ll2.transform_snapshot(d0, pipeline="test_snapshot")          # pulihkan snapshot asli (geser_jam NULL)
    for t in ("meta.dq_result", "meta.quality_gate", "meta.lineage"):
        db.execute(f"DELETE FROM {t} WHERE batch_id IN (SELECT batch_id FROM meta.etl_batch WHERE pipeline='test_snapshot')")
    db.execute("DELETE FROM meta.alert WHERE message LIKE '%%test_snapshot%%' OR message LIKE '%%ll2_upcoming batch%%'"
               " AND created_at > now() - interval '10 minutes'")
    db.execute("DELETE FROM meta.etl_batch WHERE pipeline='test_snapshot'")


def _shifted(d):
    return db.execute("""SELECT COUNT(*) FILTER (WHERE is_bergeser=1), MIN(geser_jam) FILTER (WHERE is_bergeser=1),
                                MAX(geser_jam) FILTER (WHERE is_bergeser=1)
                         FROM staging.stg_jadwal_clean WHERE snapshot_date=%s""", (d,), fetch=True)[0]


def main():
    d0 = db.scalar("SELECT MAX(snapshot_date) FROM staging.stg_ll2_launch_raw WHERE endpoint='upcoming' AND batch_id > 0")
    d1, d2 = d0 - timedelta(days=1), d0 - timedelta(days=2)
    assert not ll2.pending_snapshots(), "prasyarat: tidak boleh ada snapshot tertunda sebelum uji"
    try:
        # Skenario A
        # D-2: net 48 jam lebih awal, D-1: 24 jam lebih awal -> di D jadwal tsb mundur 24 jam vs D-1
        _insert_copy(d0, d2, 48)
        _insert_copy(d0, d1, 24)
        ll2.transform_snapshot(d2, pipeline="test_snapshot")       # D-2 sukses (run 2 hari lalu)
        db.execute("DELETE FROM staging.stg_jadwal_clean WHERE snapshot_date=%s", (d1,))   # D-1: transform "gagal"
        db.execute("UPDATE staging.stg_ll2_launch_raw SET extracted_at = now() WHERE batch_id=%s AND snapshot_date=%s",
                   (TEST_BATCH, d1))
        pend = ll2.pending_snapshots()
        assert pend == [d1], f"A: snapshot tertunda seharusnya [{d1}], didapat {pend}"
        ll2.transform_upcoming(pipeline="test_snapshot")
        assert not ll2.pending_snapshots(), "A: masih ada snapshot tertunda setelah transform_upcoming"
        n, lo, hi = _shifted(d0)
        assert n == SHIFT_IDS and float(lo) == float(hi) == 24.0, f"A: geser_jam {d0} salah: n={n} min={lo} max={hi}"
        print(f"[PASS] Skenario A: snapshot {d1} yang tertunda diproses, lalu {d0} dihitung ulang "
              f"-> {n} jadwal bergeser +24 jam")

        # Skenario B
        db.execute("DELETE FROM staging.stg_ll2_launch_raw WHERE batch_id=%s AND snapshot_date=%s", (TEST_BATCH, d1))
        db.execute("DELETE FROM staging.stg_jadwal_clean WHERE snapshot_date=%s", (d1,))
        _insert_copy(d0, d1, 0, bad_status=True)
        try:
            ll2.transform_upcoming(pipeline="test_snapshot")
            raise AssertionError("B: seharusnya QualityGateBlocked")
        except QualityGateBlocked as exc:
            print(f"[PASS] Skenario B: task gagal dengan QualityGateBlocked -> {exc}")
        assert ll2.pending_snapshots() == [d1], "B: snapshot yang diblok harus tetap tertunda"
        n, lo, hi = _shifted(d0)
        assert n == SHIFT_IDS and float(lo) == 48.0, f"B: {d0} seharusnya dibandingkan ke {d2} (+48 jam), didapat {lo}"
        print(f"[PASS] Skenario B: snapshot {d0} tetap diproses (geser +48 jam vs {d2}); {d1} tetap tertunda")
    finally:
        _cleanup(d0, [d1, d2])
        assert not ll2.pending_snapshots(), "cleanup gagal: masih ada snapshot tertunda"
        print("[OK] data uji dibersihkan, snapshot asli dipulihkan")

    # Skenario C
    before = not_yet_ran_today("test_guard_dag")
    bid = db.scalar("""INSERT INTO meta.etl_batch (pipeline, task, status) VALUES ('test_guard_dag','load_facts','SUCCESS')
                       RETURNING batch_id""")
    after = not_yet_ran_today("test_guard_dag")
    db.execute("DELETE FROM meta.etl_batch WHERE batch_id=%s", (bid,))
    db.execute("DELETE FROM meta.alert WHERE dag_id='test_guard_dag'")
    assert before is True and after is False, f"C: guard salah (sebelum={before}, sesudah={after})"
    print("[PASS] Skenario C: guard lanjut sebelum ETL sukses hari ini, skip sesudahnya")


if __name__ == "__main__":
    main()
