-- 1) Promedio final por escuela y asignatura
SELECT
  school,
  subject,
  ROUND(AVG(G3), 2)   AS avg_final,
  COUNT(*)            AS total_students,
  ROUND(STDDEV(G3),2) AS stddev_final
FROM read_parquet('data/gold/*.parquet')
GROUP BY school, subject
ORDER BY avg_final DESC;

-- 2) Correlaciones entre notas
SELECT
  corr(G1, G3) AS corr_g1_g3,
  corr(G2, G3) AS corr_g2_g3,
  corr(G1, G2) AS corr_g1_g2
FROM read_parquet('data/gold/*.parquet');

-- 3) Percentiles de G3 por asignatura
SELECT
  subject,
  quantile_cont(G3, 0.10) AS p10,
  quantile_cont(G3, 0.50) AS p50,
  quantile_cont(G3, 0.90) AS p90,
  COUNT(*)                AS n
FROM read_parquet('data/gold/*.parquet')
GROUP BY subject
ORDER BY subject;
