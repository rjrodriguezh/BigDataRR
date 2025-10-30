import os
from glob import glob
from datetime import datetime

import duckdb
import pandas as pd

DATA_GOLD = "data/gold"
REPORTS_DIR = "data/reports"

os.makedirs(REPORTS_DIR, exist_ok=True)

parquet_files = glob(os.path.join(DATA_GOLD, "*.parquet"))
if not parquet_files:
    raise FileNotFoundError(
        "No se encontraron archivos .parquet en '" + DATA_GOLD + "'. "
        "Verifica que la Semana 2 haya generado la capa Gold."
    )

con = duckdb.connect(database=':memory:')
ts = datetime.now().strftime("%Y%m%d_%H%M")

def save(df: pd.DataFrame, stem: str):
    csv_path = os.path.join(REPORTS_DIR, f"{stem}_{ts}.csv")
    df.to_csv(csv_path, index=False)
    print(f"✔ CSV -> {csv_path}")
    return csv_path

kpi_avg = con.execute(
    """
    SELECT
        school,
        subject,
        ROUND(AVG(G3), 2)   AS avg_final,
        COUNT(*)            AS total_students,
        ROUND(STDDEV(G3),2) AS stddev_final
    FROM read_parquet('data/gold/*.parquet')
    GROUP BY school, subject
    ORDER BY avg_final DESC
    """
).df()
save(kpi_avg, "kpi_avg_by_school_subject")

kpi_corr = con.execute(
    """
    SELECT
        corr(G1, G3) AS corr_g1_g3,
        corr(G2, G3) AS corr_g2_g3,
        corr(G1, G2) AS corr_g1_g2
    FROM read_parquet('data/gold/*.parquet')
    """
).df()
save(kpi_corr, "kpi_corr")

kpi_pct = con.execute(
    """
    SELECT
        subject,
        quantile_cont(G3, 0.10) AS p10,
        quantile_cont(G3, 0.50) AS p50,
        quantile_cont(G3, 0.90) AS p90,
        COUNT(*)                AS n
    FROM read_parquet('data/gold/*.parquet')
    GROUP BY subject
    ORDER BY subject
    """
).df()
save(kpi_pct, "kpi_percentiles")

html_path = os.path.join(REPORTS_DIR, f"report_{ts}.html")
parts = []
parts.append("<h2>KPI 1 — Promedio final por escuela y asignatura</h2>")
parts.append(kpi_avg.to_html(index=False))
parts.append("<h2>KPI 2 — Correlaciones entre notas</h2>")
parts.append(kpi_corr.to_html(index=False))
parts.append("<h2>KPI 3 — Percentiles de G3 por asignatura</h2>")
parts.append(kpi_pct.to_html(index=False))
with open(html_path, "w", encoding="utf-8") as f:
    f.write("\n".join(parts))
print(f"✔ HTML -> {html_path}")
print("\n✅ Semana 3 completada: KPIs generados y exportados.")
