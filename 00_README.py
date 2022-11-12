# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Databricks ETL/ML/BI EndToEnd デモ
# MAGIC 
# MAGIC 本コンテンツをクローン頂き、誠に有難うございます。本コンテンツでは、Azure Databricks を利用する上で、データ活用シナリオを促進するためのサンプル簡易コードを提供致します。本コードをベースに、デモコンテンツの整備、及び新規開発ワークロードの指針・参考になりましたら幸いです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## コンテンツ・アジェンダ  
# MAGIC 
# MAGIC - 00_README (本 notebook)
# MAGIC - 01_ETL_PerfTuning_Demo
# MAGIC - 02_ML_EndToEnd_Demo
# MAGIC - 03_SQL_BI_Dashboard_Demo

# COMMAND ----------

# MAGIC %md
# MAGIC ### 01_ETL_PerfTuning_Demo
# MAGIC - 01_Create_DemoData
# MAGIC - 02_SQL_Qeries

# COMMAND ----------

# MAGIC %md
# MAGIC ### 02_ML_EndToEnd_Demo
# MAGIC - 01_ML_EndToEnd (Create DemoData)
# MAGIC - 02_AutoML_GUI_実行チュートリアル
# MAGIC - 03_Predict_(DLTInference_Streaming)
# MAGIC - 04_Job_INSERT_TestData_LoanRisk001

# COMMAND ----------

# MAGIC %md
# MAGIC ### 03_SQL_BI_Dashboard_Demo
# MAGIC - 01_Create_Dashboard

# COMMAND ----------

# MAGIC %md
# MAGIC ## 参照リファレンス

# COMMAND ----------

# MAGIC %md
# MAGIC ### ETL (Data Engineering)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/optimizations/
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/optimizations/dynamic-file-pruning
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/delta/best-practices
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/delta/optimize

# COMMAND ----------

# MAGIC %md
# MAGIC ### ML (Machine Learning)

# COMMAND ----------

# MAGIC %md
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl/how-automl-works
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl/train-ml-model-automl-ui
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/manage-model-lifecycle/
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/workflows/

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL & BI

# COMMAND ----------

# MAGIC %md
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/scenarios/what-is-azure-databricks-sql
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/sql/admin/sql-endpoints
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/sql/admin/serverless
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/sql/user/queries/queries
# MAGIC - https://learn.microsoft.com/ja-jp/azure/databricks/sql/user/dashboards/
