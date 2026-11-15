"""Menjalankan query validasi sql/validation/dw_validation.sql setelah load DWH."""
import re

from .. import config, db
from ..runlog import track_batch
from .checks import DQ, QualityGateBlocked

CHECK_RE = re.compile(r"^-- check:\s*(?P<rule>[^|]+)\|\s*(?P<dim>[^|]+)\|\s*(?P<sev>[^|]+)\|\s*(?P<name>.+)$", re.M)


def parse_checks(path=None):
    text = (path or config.PROJECT_DIR / "sql" / "validation" / "dw_validation.sql").read_text(encoding="utf-8")
    marks = list(CHECK_RE.finditer(text))
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(text)
        yield (m["rule"].strip(), m["dim"].strip(), m["sev"].strip(), m["name"].strip(),
               text[m.end():end].strip().rstrip(";"))


def validate_dw(pipeline="orbit_daily_etl", fail_on_error=True):
    with track_batch(pipeline, "validate_dw", "dw") as b:
        dq = DQ(b.batch_id, "dw", "dw.*")
        for rule, dim, sev, name, sql in parse_checks():
            failed, total = db.execute(sql, fetch=True)[0]
            dq.check(rule, name, dim, failed or 0, total or 0, sev,
                     threshold_pct=100 if sev == "INFO" else 0.0)
        dq.write()
        b.rows_read = len(dq.results)
        failed = [r for r in dq.results if r["status"] == "FAIL"]
        b.rows_rejected = len(failed)
        b.note(f"{len(dq.results)} pemeriksaan, {len(failed)} gagal "
               f"({sum(r['severity'] == 'ERROR' for r in failed)} ERROR)")
        if fail_on_error and dq.errors_failed:
            raise QualityGateBlocked("validasi DWH gagal: "
                                     f"{[r['rule_id'] for r in failed if r['severity'] == 'ERROR']}")
        return dq.results
