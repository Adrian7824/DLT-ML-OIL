import dlt
from pyspark.sql.functions import col

SOURCE = spark.conf.get("source")  

# ---------- Producción diaria ----------
@dlt.table(
  name="bronze_well_daily_production",
  comment="Ingesta cruda de producción de pozos con Auto Loader",
  table_properties={"quality":"bronze"}
)
@dlt.expect("valid_oil", "aceite_bbl >= 0")
@dlt.expect_or_drop("not_null_pozo", "pozo_id IS NOT NULL")
def bronze_well_daily_production():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes","true")
        .load(f"{SOURCE}/sample_well_daily_production")
        .withColumn("_source", col("_metadata.file_path"))   # UC-friendly
    )

# ---------- Opex/Capex ----------
@dlt.table(
  name="bronze_opex_capex",
  comment="Ingesta de costos opex/capex",
  table_properties={"quality":"bronze"}
)
def bronze_opex_capex():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes","true")
        .load(f"{SOURCE}/sample_opex_capex")
        .withColumn("_source", col("_metadata.file_path"))
    )

# ---------- Precio Brent ----------
@dlt.table(
  name="bronze_price_brent",
  comment="Ingesta de precio mensual Brent",
  table_properties={"quality":"bronze"}
)
def bronze_price_brent():
    return (
      spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("header","true")
        .option("cloudFiles.inferColumnTypes","true")
        .load(f"{SOURCE}/sample_price_brent")
        .withColumn("_source", col("_metadata.file_path"))
    )
