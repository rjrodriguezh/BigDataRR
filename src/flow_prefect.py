# src/flow_prefect.py
# -*- coding: utf-8 -*-
import os, subprocess, sys, pathlib, datetime
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from typing import Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
REPORTS = ROOT / "reports"
LOGS = ROOT / "logs"

PY = sys.executable  # python actual

def run_py(script: str, *args: str) -> None:
    """Ejecuta un script Python como subproceso y falla si sale != 0."""
    cmd = [PY, str(SRC / script), *args]
    proc = subprocess.run(cmd, cwd=str(ROOT))
    if proc.returncode != 0:
        raise RuntimeError(f"Fallo al ejecutar: {' '.join(cmd)}")

@task(retries=2, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=datetime.timedelta(minutes=5))
def step_make_gold() -> None:
    logger = get_run_logger()
    logger.info("Generating GOLD...")
    run_py("make_gold.py")
    logger.info("OK GOLD")

@task(retries=2, retry_delay_seconds=10)
def step_make_reports() -> pathlib.Path:
    logger = get_run_logger()
    logger.info("Generating reports (CSV/HTML/PNG)...")
    run_py("make_reports.py")
    # devuelve el último HTML generado
    latest = max(REPORTS.glob("report_daily_*.html"), key=lambda p: p.stat().st_mtime)
    logger.info(f"Reports done: {latest}")
    return latest

@task(retries=0)
def step_run_dq(enable_uniqueness: bool = False) -> dict:
    logger = get_run_logger()
    env = os.environ.copy()
    env["DQ_ENABLE_UNIQUENESS"] = "1" if enable_uniqueness else "0"
    cmd = [PY, str(SRC / "run_dq.py")]
    logger.info(f"Running DQ (uniqueness={enable_uniqueness}) ...")
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True)
    # refleja salida en consola
    print(proc.stdout)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)
    # parseo mínimo: detectar PASS/FAIL desde stdout
    status = "FAIL" if "Estado           : FAIL" in proc.stdout else "PASS"
    logger.info(f"DQ Status: {status}")
    # path del runs_log
    runs_log = LOGS / "runs_log.csv"
    return {"status": status, "runs_log": str(runs_log)}

@flow(name="edu-data-platform-pipeline")
def pipeline(enable_uniqueness: bool = False, stop_on_fail: bool = True) -> dict:
    """
    Orquesta: GOLD -> REPORTES -> DQ
    - enable_uniqueness: activa regla de unicidad del DQ (opcional)
    - stop_on_fail: si True y DQ=FAIL, corta el flujo con error
    """
    make_gold = step_make_gold.submit()
    make_reports = step_make_reports.submit(wait_for=[make_gold])
    dq_res = step_run_dq.submit(enable_uniqueness, wait_for=[make_reports]).result()

    if stop_on_fail and dq_res["status"] == "FAIL":
        raise RuntimeError("Pipeline detenido por Data Quality FAIL")

    return {
        "dq_status": dq_res["status"],
        "runs_log": dq_res["runs_log"],
        "last_report_html": str(max(REPORTS.glob("report_daily_*.html"), key=lambda p: p.stat().st_mtime))
            if REPORTS.exists() else "",
    }

if __name__ == "__main__":
    # Por defecto: sin unicidad, corta si FAIL
    print(pipeline(enable_uniqueness=False, stop_on_fail=True))
