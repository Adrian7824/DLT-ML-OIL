-- 1) Producción mensual por pozo (para línea apilada o series múltiples)
SELECT pozo_id, mes, oil_bbl
FROM oil.dev.gold_monthly_kpis_tbl
ORDER BY pozo_id, mes;

-- 2) Lifting cost promedio por mes (para área/linea)
SELECT mes, AVG(lifting_cost_usd_bbl) AS avg_lifting_cost
FROM oil.dev.gold_monthly_kpis_tbl
GROUP BY mes
ORDER BY mes;

-- 3) Margen mensual por pozo (para heatmap o barras)
SELECT pozo_id, mes, margen_usd
FROM oil.dev.gold_monthly_kpis_tbl
ORDER BY pozo_id, mes;

-- 4) Ranking de pozos por margen acumulado (para barra horizontal)
SELECT pozo_id, SUM(margen_usd) AS margen_acum
FROM oil.dev.gold_monthly_kpis_tbl
GROUP BY pozo_id
ORDER BY margen_acum DESC;

-- 5) Panel de salud (GOR y rentabilidad) — tabla resumen
SELECT
  pozo_id,
  mes,
  gor,
  rentabilidad_pct,
  oil_bbl,
  opex_usd,
  precio_usd_bbl
FROM oil.dev.gold_monthly_kpis_tbl
ORDER BY pozo_id, mes;

-- 6) Vista para comparar últimos 3 meses por pozo (útil para filtros)
CREATE OR REPLACE VIEW oil.dev.v_oil_last3 AS
SELECT *
FROM (
  SELECT
    pozo_id, mes, oil_bbl, margen_usd, lifting_cost_usd_bbl, gor, rentabilidad_pct,
    ROW_NUMBER() OVER (PARTITION BY pozo_id ORDER BY mes DESC) AS rn
  FROM oil.dev.gold_monthly_kpis_tbl
)
WHERE rn <= 3;

-- 7) (Opcional) Vista de KPIs agregados por mes (para tarjetas)
CREATE OR REPLACE VIEW oil.dev.v_oil_kpi_cards AS
SELECT
  mes,
  SUM(oil_bbl) AS total_oil_bbl,
  SUM(opex_usd) AS total_opex,
  SUM(margen_usd) AS total_margen
FROM oil.dev.gold_monthly_kpis_tbl
GROUP BY mes
ORDER BY mes;
