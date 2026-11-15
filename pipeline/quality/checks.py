"""Data Quality framework + Quality Gate.

Setiap pemeriksaan menghasilkan satu baris meta.dq_result. Quality gate (TV-08) mem-BLOCK data sebuah
sumber kalau reject > 5% atau ada pemeriksaan severity ERROR yang gagal.
"""
import pandas as pd

from .. import config, db
from ..runlog import alert, log


class QualityGateBlocked(RuntimeError):
    """Data tidak lolos quality gate, load ke DWH dibatalkan."""


class DQ:
    def __init__(self, batch_id, layer, table):
        self.batch_id, self.layer, self.table = batch_id, layer, table
        self.results = []

    def check(self, rule_id, name, dimension, failed, total, severity="ERROR", threshold_pct=0.0, details=None):
        failed, total = int(failed), int(total)
        pct = round(100.0 * failed / total, 3) if total else 0.0
        status = "PASS" if pct <= threshold_pct else "FAIL"
        self.results.append(dict(batch_id=self.batch_id, layer=self.layer, table_name=self.table,
                                 rule_id=rule_id, check_name=name, dimension=dimension, severity=severity,
                                 status=status, total_rows=total, failed_rows=failed, fail_pct=pct,
                                 threshold_pct=threshold_pct, details=details))
        lvl = log.info if status == "PASS" else log.warning
        lvl("DQ %-6s %-5s %s: %s/%s gagal (%.3f%%)", rule_id, status, name, failed, total, pct)
        return status == "PASS"

    # helper pemeriksaan umum pada DataFrame
    def completeness(self, df, cols, rule_id="DQ-01", severity="ERROR"):
        for c in cols:
            self.check(rule_id, f"{c} tidak boleh kosong", "completeness", df[c].isna().sum(), len(df), severity)

    def uniqueness(self, df, cols, rule_id="TV-07"):
        dup = df.duplicated(subset=cols, keep=False).sum()
        self.check(rule_id, f"kunci unik {'+'.join(cols)}", "uniqueness", dup, len(df))

    def in_range(self, series, lo, hi, name, rule_id, severity="ERROR"):
        s = pd.to_numeric(series, errors="coerce")
        bad = s.notna() & ((s < lo) | (s > hi))
        self.check(rule_id, name, "validity", bad.sum(), int(s.notna().sum()), severity)

    def in_set(self, series, allowed, name, rule_id, severity="ERROR"):
        bad = series.notna() & ~series.isin(allowed)
        self.check(rule_id, name, "validity", bad.sum(), len(series), severity,
                   details=", ".join(sorted(map(str, series[bad].unique())))[:500] or None)

    @property
    def errors_failed(self):
        return sum(1 for r in self.results if r["status"] == "FAIL" and r["severity"] == "ERROR")

    def write(self):
        if not self.results:
            return 0
        with db.transaction() as conn, conn.cursor() as cur:
            return db.insert_df(cur, "meta.dq_result", pd.DataFrame(self.results))


def quality_gate(batch_id, source, total_rows, rejected_rows, dq: DQ | None = None, raise_on_block=True):
    pct = round(100.0 * rejected_rows / total_rows, 3) if total_rows else 0.0
    err = dq.errors_failed if dq else 0
    decision = "PASS" if pct <= config.QUALITY_GATE_REJECT_PCT and err == 0 else "BLOCK"
    db.execute("""INSERT INTO meta.quality_gate (batch_id, source, total_rows, rejected_rows, reject_pct,
                      threshold_pct, error_checks_failed, decision) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
               (batch_id, source, total_rows, rejected_rows, pct, config.QUALITY_GATE_REJECT_PCT, err, decision))
    log.info("QUALITY GATE %s: %s (reject %.3f%%, error checks gagal %s)", source, decision, pct, err)
    if decision == "BLOCK":
        msg = (f"Quality gate BLOCK untuk {source} batch {batch_id}: reject {pct}% "
               f"(ambang {config.QUALITY_GATE_REJECT_PCT}%), {err} pemeriksaan ERROR gagal")
        alert("CRITICAL", msg, "quality_gate", source, notify=True)
        if raise_on_block:
            raise QualityGateBlocked(msg)
    return decision
