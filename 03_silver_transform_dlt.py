import dlt
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

# -----------------------------
# Producción diaria (Wells)
# -----------------------------
@dlt.table(
    name="silver_well_daily_production",
    comment="Producción limpia y tipificada por pozo y fecha",
    table_properties={"quality": "silver"}
)
@dlt.expect("not_null_pozo", "pozo_id IS NOT NULL")
@dlt.expect("not_null_fecha", "fecha IS NOT NULL")
@dlt.expect("oil_non_negative", "aceite_bbl >= 0")
def silver_well_daily_production():
    df = (
        dlt.read_stream("bronze_well_daily_production")
        .withColumn("fecha", to_date(col("fecha")))
        .withColumn("aceite_bbl", col("aceite_bbl").cast(DoubleType()))
        .withColumn("gas_m3", col("gas_m3").cast(DoubleType()))
        .withColumn("agua_bbl", col("agua_bbl").cast(DoubleType()))
    )
    # Deduplicación conservadora por clave natural
    return df.dropDuplicates(["pozo_id", "fecha"])

# -----------------------------
# OPEX / CAPEX
# -----------------------------
@dlt.table(
    name="silver_opex_capex",
    comment="Costos operativos y de capital limpios por pozo y fecha",
    table_properties={"quality": "silver"}
)
@dlt.expect("not_null_pozo", "pozo_id IS NOT NULL")
@dlt.expect("not_null_fecha", "fecha IS NOT NULL")
@dlt.expect("opex_non_negative", "opex_usd >= 0")
@dlt.expect("capex_non_negative", "capex_usd >= 0")
def silver_opex_capex():
    df = (
        dlt.read_stream("bronze_opex_capex")
        .withColumn("fecha", to_date(col("fecha")))
        .withColumn("opex_usd", col("opex_usd").cast(DoubleType()))
        .withColumn("capex_usd", col("capex_usd").cast(DoubleType()))
    )
    return df.dropDuplicates(["pozo_id", "fecha"])

# -----------------------------
# Precio Brent
# -----------------------------
@dlt.table(
    name="silver_price_brent",
    comment="Serie de precio Brent limpia por fecha (mensual)",
    table_properties={"quality": "silver"}
)
@dlt.expect("not_null_fecha", "fecha IS NOT NULL")
@dlt.expect("price_positive", "precio_usd_bbl > 0")
def silver_price_brent():
    df = (
        dlt.read_stream("bronze_price_brent")
        .withColumn("fecha", to_date(col("fecha")))
        .withColumn("precio_usd_bbl", col("precio_usd_bbl").cast(DoubleType()))
    )
    # En precio por fecha, deduplicar solo por fecha
    return df.dropDuplicates(["fecha"])