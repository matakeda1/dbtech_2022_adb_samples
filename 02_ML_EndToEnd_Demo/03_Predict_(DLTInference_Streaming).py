# Databricks notebook source
# MAGIC %pip install mlflow

# COMMAND ----------

# MAGIC %md
# MAGIC This is an auto-generated notebook to perform Streaming inference in a Delta Live Tables pipeline using a selected model from the model registry. This feature is in preview.
# MAGIC 
# MAGIC ## Instructions:
# MAGIC Please [add](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-ui.html#edit-settings) this notebook to your Delta Live Tables pipeline as an additional notebook library,
# MAGIC or [create](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-ui.html#create-a-pipeline) a new pipeline with this notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. 基本設定：テーブル名 / データベース名
# MAGIC 
# MAGIC 利用するデータベース名を設定しましょう (テーブル名は、前段ノートブックで生成した loan_status_input テーブルを指定)
# MAGIC 
# MAGIC The cell below retrieves the Delta Live Tables table name and table schema from the given Delta table.

# COMMAND ----------

from delta.tables import *

input_delta_table = DeltaTable.forName(spark, "hive_metastore.<データベース名>.loan_stats_input")

# The Delta Live Tables table name for the input table that will be used in the pipeline code below.
input_dlt_table_name = "<データベース名>.loan_stats_input"

# The input table schema stored as an array of strings. This is used to pass in the schema to the model predict udf.
input_dlt_table_columns = input_delta_table.toDF().columns

# COMMAND ----------

# MAGIC %md ## 2. Load model as UDF and restore environment
# MAGIC 
# MAGIC 先ほど登録したモデル名を、以下に設定しましょう。　　
# MAGIC 
# MAGIC 最後の /1 はバージョン (ver.1) に設定しています。別のバージョンを指定する際には、この値を変更してください。
# MAGIC 
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

import mlflow

model_uri = f"models:/<登録したモデル名>/1"

# create spark user-defined function for model prediction.
# Note: : Here we use virtualenv to restore the python environment that was used to train the model.
predict = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="double", env_manager='virtualenv')

# COMMAND ----------

# MAGIC %md ## 3. Run inference & Write predictions to output
# MAGIC 
# MAGIC データベース名を設定しましょう
# MAGIC 
# MAGIC **Note**: If you want to rename the output Delta table in the pipeline later on, please rename the function below: `dbtech_model_predictions()`.

# COMMAND ----------

import dlt
from pyspark.sql.functions import struct

## ADD
@dlt.table(
  spark_conf={"pipelines.trigger.interval" : "10 seconds"}
)
def dlt_loan_stats_input():
  return spark.table(input_dlt_table_name)
## ADD


@dlt.table(
  spark_conf={"pipelines.trigger.interval" : "10 seconds"},
  comment="DLT for predictions scored by dbtech_model model based on hive_metastore.<データベース名>.loan_stats_input Delta table.",
  table_properties={
    "quality": "gold"
  }
)
def dbtech_model_predictions():
  return (
    dlt.read("dlt_loan_stats_input")
    .withColumn('prediction', predict(struct(input_dlt_table_columns)))
  )


# COMMAND ----------

# MAGIC %md 
# MAGIC ## 4. ワークフロー (Delta Live Table) の設定
# MAGIC 
# MAGIC "ワークフロー"タブを選択し、Delta Live Tables を選択し、本ノートブックを指定・設定しましょう

# COMMAND ----------

# MAGIC %md
# MAGIC ![image](https://user-images.githubusercontent.com/38490168/201460018-9a60cfbd-1ede-4d78-a3bc-b220a790591f.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. 推論 (DeltaLiveTables) の実行
# MAGIC 
# MAGIC 上手く実行されると、以下のようにリアルタイムに処理実行できていることが確認できます。
# MAGIC 
# MAGIC "04_Job_INSERT_TestData_LoanRisk001" というノートブックに記載のコマンドをコピーして SQL Warehouse に対して実行すると、都度推論実行の内容が反映される仕組みになっています。

# COMMAND ----------

# MAGIC %md
# MAGIC ![image](https://user-images.githubusercontent.com/38490168/201460426-f05c76ed-4b97-4dab-9fd4-7d8c7a0e492b.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. 融資の新規申込シナリオの実施 (SQLクエリでシミュレーション)

# COMMAND ----------

# MAGIC %md
# MAGIC ![image](https://user-images.githubusercontent.com/38490168/201460492-39d7ba0c-c9f2-4341-923e-6638ee06e356.png)

# COMMAND ----------


