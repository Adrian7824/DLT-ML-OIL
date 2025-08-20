import os
import mlflow
import mlflow.sklearn
import pandas as pd
from mlflow.tracking import MlflowClient
from pyspark.sql.functions import col, max as _max

mlflow.set_registry_uri("databricks") 

EXPERIMENT_PATH = "/Shared/oil_ml_experiment"
mlflow.set_experiment(EXPERIMENT_PATH)

# FEATURES que espera el modelo de regresión
cols_feat = [
    "oil_bbl_lag1","oil_bbl_lag2","oil_bbl_lag3",
    "oil_bbl_ma3","oil_bbl_ma6","oil_bbl_std3",
    "lifting_ma3","gor_ma3"
]

exp = mlflow.get_experiment_by_name(EXPERIMENT_PATH)
if exp is None:
    raise RuntimeError(f"No existe el experimento: {EXPERIMENT_PATH}. Ejecuta la Task 3 (train).")

client = MlflowClient()

best_runs = client.search_runs(
    experiment_ids=[exp.experiment_id],
    filter_string="attributes.status = 'FINISHED' and tags.mlflow.runName = 'rf_regression_oil_next'",
    order_by=["metrics.rmse ASC"],
    max_results=1
)

if not best_runs:
    # Fallback robusto: traer varios, filtrar en Python y elegir el menor RMSE
    candidates = client.search_runs(
        experiment_ids=[exp.experiment_id],
        filter_string="attributes.status = 'FINISHED'",
        order_by=["attributes.start_time DESC"],
        max_results=100
    )
    # Mantener solo runs con métrica rmse
    candidates = [r for r in candidates if "rmse" in r.data.metrics]
    if not candidates:
        raise RuntimeError("No hay runs con métrica RMSE. Ejecuta la Task 3 (train) primero.")
    best = min(candidates, key=lambda r: r.data.metrics["rmse"])
else:
    best = best_runs[0]

best_run_id = best.info.run_id
model_uri = f"runs:/{best_run_id}/model_regression"
print("Usando RUN_ID:", best_run_id)

# 2) Carga modelo y features del último mes
model = mlflow.sklearn.load_model(model_uri)

feat_spark = spark.table("oil.dev.gold_features_ml")
last_mes = feat_spark.select(_max(col("mes")).alias("m")).collect()[0]["m"]
if last_mes is None:
    raise RuntimeError("No hay registros en gold_features_ml. Ejecuta la Task 2 (features).")

to_score = (feat_spark
            .filter(col("mes") == last_mes)
            .select("pozo_id","mes", *cols_feat)
            .dropna(how="any")        # garantiza entradas completas al modelo
            .toPandas())

if to_score.empty:
    raise RuntimeError("No hay filas para score (posiblemente todo NaN en el último mes). Revisa features.")

# 3) Predice y guarda resultados
to_score["y_hat_oil_bbl_next"] = model.predict(to_score[cols_feat])
pred_spark = spark.createDataFrame(to_score)

# Escribe tabla Delta (idempotente por mes)
(pred_spark
 .write
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("oil.dev.gold_predictions"))

print("Scoring listo → oil.dev.gold_predictions (mes:", last_mes, ")")
