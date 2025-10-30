# src/make_reports.py
# -*- coding: utf-8 -*-
import os, glob, textwrap, pathlib
from datetime import datetime
import duckdb
import pandas as pd
import matplotlib.pyplot as plt

# Rutas base
PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Timestamp y salidas
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
csv_path = REPORTS_DIR / f"report_daily_{ts}.csv"
html_path = REPORTS_DIR / f"report_daily_{ts}.html"

# Diagnóstico
print(f"cwd: {os.getcwd()}")
print(f"GOLD_DIR: {GOLD_DIR}")

# Buscar .parquet (recursivo y plano)
patterns = [str(GOLD_DIR / "**" / "*.parquet"), str(GOLD_DIR / "*.parquet")]
files = []
for pat in patterns:
    found = glob.glob(pat, recursive=True)
    if found:
        files.extend(found)

# Normalizar y deduplicar rutas
files = sorted(set([str(pathlib.Path(f)) for f in files]))

if not files:
    raise FileNotFoundError(textwrap.dedent(f"""
    No se encontraron Parquet.
      - Buscado: {patterns[0]} y {patterns[1]}
      - Verifica que exista: {GOLD_DIR}\\student_all.parquet
    """).strip())

print(f"✅ Parquet encontrados ({len(files)}):")
for f in files:
    print("  -", f)

# Conexión y VISTA "gold" sin parámetros preparados:
# Opción segura: usar la API Python y registrar la relación directamente.
con = duckdb.connect(database=":memory:")
rel = con.read_parquet(files, filename=True)  # usa la MISMA conexión
con.register("gold", rel)

# =======================
# Consultas KPI (SQL)
# =======================
sql_avg = """
SELECT
  school,
  subject,
  ROUND(AVG(G3), 2) AS avg_g3,
  COUNT(*) AS n
FROM gold
GROUP BY school, subject
ORDER BY school, subject;
"""

sql_corr_overall = """
SELECT corr(G1, G3) AS corr_g1_g3_overall
FROM gold;
"""

sql_corr_by_subject = """
SELECT subject, corr(G1, G3) AS corr_g1_g3
FROM gold
GROUP BY subject
ORDER BY subject;
"""

sql_percentiles = """
SELECT
  subject,
  quantile_cont(G3, 0.10) AS p10,
  quantile_cont(G3, 0.25) AS p25,
  quantile_cont(G3, 0.50) AS p50,
  quantile_cont(G3, 0.75) AS p75,
  quantile_cont(G3, 0.90) AS p90
FROM gold
GROUP BY subject
ORDER BY subject;
"""

sql_ranking = """
WITH ranked AS (
  SELECT
    school, sex, age, subject, G1, G2, G3,
    ROW_NUMBER() OVER (PARTITION BY subject ORDER BY G3 DESC, G2 DESC, G1 DESC) AS rk
  FROM gold
)
SELECT *
FROM ranked
WHERE rk <= 10
ORDER BY subject, rk;
"""

# Ejecutar y recolectar DataFrames
df_avg = con.execute(sql_avg).fetchdf()
df_corr_overall = con.execute(sql_corr_overall).fetchdf()
df_corr_subject = con.execute(sql_corr_by_subject).fetchdf()
df_pct = con.execute(sql_percentiles).fetchdf()
df_rank = con.execute(sql_ranking).fetchdf()

# Guardar CSV consolidado
def add_section(df, name):
    out = df.copy()
    out.insert(0, "section", name)
    return out

csv_union = pd.concat([
    add_section(df_avg, "avg_g3_by_school_subject"),
    add_section(df_corr_overall, "corr_overall"),
    add_section(df_corr_subject, "corr_by_subject"),
    add_section(df_pct, "percentiles_g3"),
    add_section(df_rank, "top10_g3_by_subject"),
], ignore_index=True)

csv_union.to_csv(csv_path, index=False, encoding="utf-8")

# Figuras (matplotlib, sin estilos específicos)
# 1) Barras: promedio G3 por school/subject
pivot_avg = df_avg.pivot(index="school", columns="subject", values="avg_g3")
plt.figure()
pivot_avg.plot(kind="bar")
plt.title("Promedio G3 por school / subject")
plt.xlabel("school"); plt.ylabel("avg_g3")
plt.tight_layout()
fig1_path = REPORTS_DIR / f"fig_avg_{ts}.png"
plt.savefig(fig1_path, dpi=120)
plt.close()

# 2) Boxplot G3 por subject
df_gold = con.execute("SELECT subject, G1, G2, G3 FROM gold").fetchdf()
plt.figure()
df_gold.boxplot(column="G3", by="subject")
plt.title("Distribución G3 por subject"); plt.suptitle("")
plt.xlabel("subject"); plt.ylabel("G3")
plt.tight_layout()
fig2_path = REPORTS_DIR / f"fig_box_{ts}.png"
plt.savefig(fig2_path, dpi=120)
plt.close()

# HTML (tablas + imágenes)
sections = [
    ("Promedio G3 por school/subject", df_avg),
    ("Correlación G1↔G3 (global)", df_corr_overall),
    ("Correlación G1↔G3 por subject", df_corr_subject),
    ("Percentiles G3 por subject", df_pct),
    ("Top 10 G3 por subject", df_rank),
]
html_parts = [f"<h1>Reporte Diario — {ts}</h1>"]
for title, df in sections:
    html_parts.append(f"<h2>{title}</h2>")
    html_parts.append(df.to_html(index=False))

html_parts.append("<h2>Figuras</h2>")
html_parts.append(f'<p><img src="{fig1_path.name}" style="max-width:100%;height:auto;" /></p>')
html_parts.append(f'<p><img src="{fig2_path.name}" style="max-width:100%;height:auto;" /></p>')

html = "\n".join(html_parts)
with open(html_path, "w", encoding="utf-8") as f:
    f.write(html)

print("OK")
print(f"CSV  → {csv_path}")
print(f"HTML → {html_path}")
