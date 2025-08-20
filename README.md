# Oil Lakehouse ML Pipeline 🚀

Este proyecto implementa un **end-to-end Lakehouse pipeline** en Databricks para el sector Oil & Gas, integrando **Delta Live Tables (DLT)**, **MLflow**, y **Dashboards SQL**.  

## 🔎 Descripción general

El objetivo es construir un flujo de datos robusto que permita:

1. **Ingesta ETL (Bronze/Silver/Gold con DLT):**  
   Lectura de datos crudos de producción de pozos, limpieza y consolidación en tablas Delta.

2. **Generación de Features ML:**  
   Cálculo de variables derivadas (lags, medias móviles, desviaciones, GOR, lifting cost, márgenes, etc.) para alimentar modelos predictivos.

3. **Entrenamiento de Modelos con MLflow:**  
   - **Regresión:** predecir producción futura (`oil_bbl_next`).  
   - **Clasificación:** identificar pozos en riesgo (margen negativo).  
   Se registran métricas (RMSE, AUC, F1) y parámetros en MLflow.

4. **Scoring batch automático:**  
   Uso del mejor modelo entrenado para predecir la producción del próximo mes y guardar los resultados en Delta.

5. **Dashboard SQL:**  
   Consultas optimizadas sobre la capa Gold para KPIs mensuales, predicciones y riesgos de pozos.

---

## 📊 Arquitectura

![Pipeline](oil_lakehouse_flow.png)

Flujo completo:  
**DLT (ETL)** → **Delta Tables** → **Features ML** → **MLflow (Train & Register)** → **Batch Scoring** → **SQL Dashboards**

---

## 🚀 Quickstart

1. Clonar el repo en Databricks Repos.
2. Configurar el volumen `oil/dev/raw` para ingesta de datos.
3. Ejecutar notebooks en orden `01 → 08`.
4. Ver resultados en:
   - `oil.dev.gold_monthly_kpis`
   - `oil.dev.gold_features_ml`
   - `oil.dev.gold_predictions`
5. Explorar dashboards con `08_dashboard_queries.sql`.

---

## 🛠️ Tech Stack

- **Databricks Lakehouse Platform**  
- **Delta Live Tables (DLT)**  
- **Apache Spark / PySpark**  
- **MLflow** (tracking + model registry)  
- **SQL Dashboards**  

---

## 📌 Próximos pasos

- Automatizar retraining programado.  
- Mejorar synthetic data generator.  
- Añadir modelos adicionales (Gradient Boosting, XGBoost).  
- Publicar dashboards interactivos en Databricks SQL.  

---

## ✍️ Autor

Proyecto desarrollado por **Adrián Gómez Rodríguez** (Data Engineer / Data Scientist).  
