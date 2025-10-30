# src/run_dq.py
# -*- coding: utf-8 -*-
import os, pathlib, textwrap, hashlib
from datetime import datetime
import duckdb
import pandas as pd

# === CONFIGURACIÓN ===
ROOT = pathlib.Path(__file__).resolve().parents[1]
GOLD_DIR = ROOT / "data" / "gold"
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

RUN_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
RUN_LOG = LOG_DIR / "runs_log.csv"
FAIL_PATH = LOG_DIR / f"dq_failures_{RUN_TS}.csv"

# === PARÁMETROS ===
THRESHOLD_FAIL_RATIO = 0.02  # 2 %
ENABLE_UNIQUENESS = os.getenv("DQ_ENABLE_UNIQUENESS", "0") == "1"
ID_COLS = ["school","sex","age","subject","G1","G2","G3","absences"]  # si se usa unicidad

# === 1) CARGA DEL GOLD ===
parquets = [str(p) for p in GOLD_DIR.rglob("*.parquet")]
if not parquets:
    raise FileNotFoundError(f"No hay archivos Parquet en {GOLD_DIR}")

con = duckdb.connect()
rel = con.read_parquet(parquets)
con.register("gold", rel)
df = con.execute("SELECT * FROM gold").fetchdf()
total_rows = len(df)
if total_rows == 0:
    raise RuntimeError("El dataset Gold está vacío.")

# === 2) REGLAS DE CALIDAD ===
checks = {}

# 2.1 No nulos
checks["not_null_school"]  = ~df["school"].isna() if "school" in df.columns else False
checks["not_null_subject"] = ~df["subject"].isna() if "subject" in df.columns else False

# 2.2 Rangos válidos para notas
for g in ["G1","G2","G3"]:
    if g in df.columns:
        checks[f"range_{g}_0_20"] = df[g].between(0, 20, inclusive="both")
    else:
        checks[f"missing_col_{g}"] = False

# 2.3 Ausencias >= 0
if "absences" in df.columns:
    checks["absences_ge_0"] = df["absences"] >= 0
else:
    checks["missing_col_absences"] = False

# 2.4 Edad entre 10 y 30
if "age" in df.columns:
    checks["age_between_10_30"] = df["age"].between(10, 30, inclusive="both")
else:
    checks["missing_col_age"] = False

# 2.5 Unicidad (opcional, activable con variable DQ_ENABLE_UNIQUENESS=1)
if ENABLE_UNIQUENESS:
    def key_hash(row):
        vals = []
        for c in ID_COLS:
            vals.append("" if c not in df.columns else str(row.get(c, "")))
        s = "|".join(vals)
        return hashlib.md5(s.encode("utf-8")).hexdigest()
    df["_student_key"] = df.apply(key_hash, axis=1)
    dup_mask = df["_student_key"].duplicated(keep="first")
    checks["unique_student_key"] = ~dup_mask

# === 3) MATRIZ DE FALLOS ===
checks_df = pd.DataFrame(checks).fillna(False).astype(bool)
any_fail = ~checks_df.all(axis=1)

failed_rows = int(any_fail.sum())
fail_ratio = failed_rows / total_rows if total_rows else 0.0
status = "PASS" if fail_ratio <= THRESHOLD_FAIL_RATIO else "FAIL"

# === 4) DETALLE DE FILAS FALLIDAS ===
if failed_rows > 0:
    detail = df.loc[any_fail].copy()
    for c in checks_df.columns:
        detail[f"check_{c}"] = checks_df.loc[detail.index, c].astype(bool)
    detail.to_csv(FAIL_PATH, index=False, encoding="utf-8")
else:
    FAIL_PATH = None

# === 5) LOG DE EJECUCIONES ===
row = {
    "run_ts": RUN_TS,
    "status": status,
    "rows": total_rows,
    "failed_rows": failed_rows,
    "failed_ratio": round(fail_ratio, 6),
    "threshold_ratio": THRESHOLD_FAIL_RATIO,
    "fail_detail_path": str(FAIL_PATH) if FAIL_PATH else "",
    "uniqueness_enabled": ENABLE_UNIQUENESS,
}
cols = [
    "run_ts","status","rows","failed_rows","failed_ratio",
    "threshold_ratio","fail_detail_path","uniqueness_enabled"
]
if RUN_LOG.exists():
    log_df = pd.read_csv(RUN_LOG)
    log_df = pd.concat([log_df, pd.DataFrame([row])], ignore_index=True)
else:
    log_df = pd.DataFrame([row], columns=cols)
log_df.to_csv(RUN_LOG, index=False, encoding="utf-8")

# === 6) RESUMEN Y DESGLOSE ===
rule_counts = checks_df.eq(False).sum().sort_values(ascending=False)
print(textwrap.dedent(f"""
DQ RUN
------
Rows totales     : {total_rows}
Filas con fallos : {failed_rows}
Ratio de fallos  : {fail_ratio:.4%}
Umbral (FAIL)    : {THRESHOLD_FAIL_RATIO:.2%}
Unicidad activa  : {ENABLE_UNIQUENESS}
Estado           : {status}
Detalle          : {FAIL_PATH if FAIL_PATH else '(sin fallos)'}
Log              : {RUN_LOG}

Desglose por regla (filas con fallo):
{rule_counts.to_string()}
""").strip())

# === (Opcional) DETENER PIPELINE EN FAIL ===
# if status == "FAIL":
#     raise SystemExit("Data Quality FAIL: supera umbral.")
