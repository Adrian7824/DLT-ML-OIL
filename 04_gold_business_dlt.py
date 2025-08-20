import dlt
from pyspark.sql.functions import col, sum as _sum, date_trunc, when, round as _round
from pyspark.sql.window import Window

@dlt.table(
    name="gold_monthly_kpis",
    comment="KPIs mensuales por pozo (producción, costos, precio, GOR, rentabilidad, acumulados)",
    table_properties={"quality": "gold"}
)
@dlt.expect("oil_non_negative", "oil_bbl >= 0")
@dlt.expect("opex_non_negative", "opex_usd >= 0")
@dlt.expect("precio_positive_or_null", "precio_usd_bbl IS NULL OR precio_usd_bbl > 0")
def gold_monthly_kpis():
    # Entradas nivel silver
    prod = dlt.read("silver_well_daily_production")
    cost = dlt.read("silver_opex_capex")
    price = dlt.read("silver_price_brent")

    # Agregados mensuales
    prod_m = (
        prod.withColumn("mes", date_trunc("month", col("fecha")))
            .groupBy("pozo_id", "mes")
            .agg(
                _sum("aceite_bbl").alias("oil_bbl"),
                _sum("gas_m3").alias("gas_m3"),
                _sum("agua_bbl").alias("agua_bbl")
            )
    )

    cost_m = (
        cost.withColumn("mes", date_trunc("month", col("fecha")))
            .groupBy("pozo_id", "mes")
            .agg(
                _sum("opex_usd").alias("opex_usd"),
                _sum("capex_usd").alias("capex_usd")
            )
    )

    price_m = (
        price.withColumn("mes", date_trunc("month", col("fecha")))
             .select("mes", "precio_usd_bbl")
             .dropDuplicates(["mes"])
    )

    # Join y métricas base
    kpis = (
        prod_m.join(cost_m, ["pozo_id", "mes"], "left")
              .join(price_m, ["mes"], "left")
    )

    # KPIs derivados (evitar divisiones por cero / nulos)
    kpis = (
        kpis
        .withColumn(
            "lifting_cost_usd_bbl",
            when(col("oil_bbl") > 0, col("opex_usd") / col("oil_bbl")).otherwise(None)
        )
        .withColumn(
            "ingreso_usd",
            when(col("precio_usd_bbl").isNotNull(), col("precio_usd_bbl") * col("oil_bbl")).otherwise(None)
        )
        .withColumn(
            "margen_usd",
            when(col("ingreso_usd").isNotNull() & col("opex_usd").isNotNull(),
                 col("ingreso_usd") - col("opex_usd")
            ).otherwise(None)
        )
        # GOR = gas / oil
        .withColumn(
            "gor",
            when(col("oil_bbl") > 0, col("gas_m3") / col("oil_bbl")).otherwise(None)
        )
        # Rentabilidad (%)
        .withColumn(
            "rentabilidad_pct",
            when(col("ingreso_usd").isNotNull() & (col("ingreso_usd") != 0),
                 (col("margen_usd") / col("ingreso_usd")) * 100
            ).otherwise(None)
        )
    )

    # Acumulados por pozo
    w_pozo = Window.partitionBy("pozo_id").orderBy("mes").rowsBetween(Window.unboundedPreceding, 0)
    kpis = (
        kpis
        .withColumn("oil_bbl_cum", _sum("oil_bbl").over(w_pozo))
        .withColumn("capex_cum", _sum("capex_usd").over(w_pozo))
    )

    # Validaciones extras razonables
    dlt.expect("gor_reasonable", "gor IS NULL OR (gor BETWEEN 100 AND 5000)")
    dlt.expect("rentabilidad_range", "rentabilidad_pct IS NULL OR (rentabilidad_pct BETWEEN -200 AND 200)")

    # Redondeo amable
    return (
        kpis
        .withColumn("oil_bbl", _round(col("oil_bbl"), 2))
        .withColumn("gas_m3", _round(col("gas_m3"), 2))
        .withColumn("agua_bbl", _round(col("agua_bbl"), 2))
        .withColumn("opex_usd", _round(col("opex_usd"), 2))
        .withColumn("capex_usd", _round(col("capex_usd"), 2))
        .withColumn("precio_usd_bbl", _round(col("precio_usd_bbl"), 2))
        .withColumn("lifting_cost_usd_bbl", _round(col("lifting_cost_usd_bbl"), 4))
        .withColumn("ingreso_usd", _round(col("ingreso_usd"), 2))
        .withColumn("margen_usd", _round(col("margen_usd"), 2))
        .withColumn("gor", _round(col("gor"), 4))
        .withColumn("rentabilidad_pct", _round(col("rentabilidad_pct"), 2))
        .withColumn("oil_bbl_cum", _round(col("oil_bbl_cum"), 2))
        .withColumn("capex_cum", _round(col("capex_cum"), 2))
    )
