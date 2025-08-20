from pyspark.sql.functions import col, lag, avg, stddev_samp, when
from pyspark.sql.window import Window

# Usamos la TABLA Delta materializada por CTAS
kpis = spark.table("oil.dev.gold_monthly_kpis_tbl")

w = Window.partitionBy("pozo_id").orderBy("mes")

feat = (
    kpis
    # Lags
    .withColumn("oil_bbl_lag1", lag("oil_bbl", 1).over(w))
    .withColumn("oil_bbl_lag2", lag("oil_bbl", 2).over(w))
    .withColumn("oil_bbl_lag3", lag("oil_bbl", 3).over(w))
    # Promedios móviles
    .withColumn("oil_bbl_ma3", avg("oil_bbl").over(w.rowsBetween(-2, 0)))
    .withColumn("oil_bbl_ma6", avg("oil_bbl").over(w.rowsBetween(-5, 0)))
    # Volatilidad corta
    .withColumn("oil_bbl_std3", stddev_samp("oil_bbl").over(w.rowsBetween(-2, 0)))
    # Señales de operación
    .withColumn("lifting_ma3", avg("lifting_cost_usd_bbl").over(w.rowsBetween(-2, 0)))
    .withColumn("gor_ma3", avg("gor").over(w.rowsBetween(-2, 0)))
    # Etiquetas
    .withColumn("margen_next", lag("margen_usd", -1).over(w))
    .withColumn("label_riesgo", when(col("margen_next") < 0, 1).otherwise(0))
    .withColumn("oil_bbl_next", lag("oil_bbl", -1).over(w))  # label para forecast
)

feat.write.mode("overwrite").saveAsTable("oil.dev.gold_features_ml")
print("Tabla creada: oil.dev.gold_features_ml")
