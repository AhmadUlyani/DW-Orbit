"""Smoke test dashboard: jalankan ke-6 halaman Streamlit secara headless (AppTest) terhadap DWH aktif.

Dijalankan di dalam container dashboard (kode dashboard ada di /app):
    docker compose exec -T dashboard python - < tests/dashboard_smoke_test.py
Gagal (exit 1) bila ada halaman yang memunculkan exception atau st.error.
"""
import os
import sys

os.chdir("/app")
sys.path.insert(0, "/app")
from streamlit.testing.v1 import AppTest

PAGES = ["views/ringkasan.py", "views/keandalan.py", "views/biaya.py",
         "views/jadwal.py", "views/model.py", "views/pipeline.py"]
bad = 0
for p in PAGES:
    at = AppTest.from_file("/app/app.py", default_timeout=120)
    at.run()
    if p != PAGES[0]:
        at.switch_page(p)
        at.run()
    exc = [e.value for e in at.exception]
    errs = [e.value for e in at.error]
    warns = [w.value for w in at.warning]
    print(f"== {p}: exceptions={len(exc)} errors={len(errs)} warnings={len(warns)} "
          f"metrics={len(at.metric)} dataframes={len(at.dataframe)} "
          f"charts={len(at.get('plotly_chart'))}")
    for m in at.metric:
        print(f"   metric {m.label!r} = {m.value!r}")
    for x in exc + errs:
        bad += 1
        print("   !!", str(x)[:1500])
    for w in warns:
        print("   warn:", str(w)[:300])
sys.exit(1 if bad else 0)
