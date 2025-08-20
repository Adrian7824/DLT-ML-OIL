import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, roc_auc_score, f1_score
from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier


mlflow.set_registry_uri("databricks") 

EXPERIMENT_PATH = "/Shared/oil_ml_experiment"
mlflow.set_experiment(EXPERIMENT_PATH)

# Carga features
df = spark.table("oil.dev.gold_features_ml").dropna().toPandas()

# -----------------------------
# REGRESIÓN: predecir oil_bbl_next
# -----------------------------
features_reg = ["oil_bbl_lag1","oil_bbl_lag2","oil_bbl_lag3","oil_bbl_ma3","oil_bbl_ma6","oil_bbl_std3","lifting_ma3","gor_ma3"]
target_reg = "oil_bbl_next"

Xr = df[features_reg]
yr = df[target_reg]

Xr_train, Xr_test, yr_train, yr_test = train_test_split(Xr, yr, test_size=0.2, random_state=42)

with mlflow.start_run(run_name="rf_regression_oil_next") as run:
    rf_reg = RandomForestRegressor(n_estimators=400, random_state=42, n_jobs=-1)
    rf_reg.fit(Xr_train, yr_train)
    yr_pred = rf_reg.predict(Xr_test)
    rmse = float(np.sqrt(mean_squared_error(yr_test, yr_pred)))
    mlflow.log_metric("rmse", rmse)
    mlflow.log_param("n_estimators", 400)
    mlflow.sklearn.log_model(rf_reg, artifact_path="model_regression")
    print("Regresión RMSE:", rmse)
    print("RUN_ID_REG:", run.info.run_id)

# -----------------------------
# CLASIFICACIÓN: pozo en riesgo (margen_next < 0)
# -----------------------------
features_clf = ["oil_bbl_lag1","oil_bbl_lag2","oil_bbl_lag3","oil_bbl_ma3","oil_bbl_ma6","oil_bbl_std3","lifting_ma3","gor_ma3"]
target_clf = "label_riesgo"

Xc = df[features_clf]
yc = df[target_clf]

Xc_train, Xc_test, yc_train, yc_test = train_test_split(Xc, yc, test_size=0.2, random_state=42, stratify=yc)

with mlflow.start_run(run_name="rf_classification_risk") as run:
    rf_clf = RandomForestClassifier(n_estimators=500, random_state=42, n_jobs=-1, class_weight="balanced")
    rf_clf.fit(Xc_train, yc_train)
    yc_prob = rf_clf.predict_proba(Xc_test)[:,1]
    yc_pred = (yc_prob >= 0.5).astype(int)

    auc = float(roc_auc_score(yc_test, yc_prob))
    f1  = float(f1_score(yc_test, yc_pred))
    mlflow.log_metric("auc", auc)
    mlflow.log_metric("f1", f1)
    mlflow.log_param("n_estimators", 500)
    mlflow.sklearn.log_model(rf_clf, artifact_path="model_classification")
    print("Clasificación AUC:", auc, "F1:", f1)
