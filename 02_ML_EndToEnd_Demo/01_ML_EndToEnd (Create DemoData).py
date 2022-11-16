# Databricks notebook source
# MAGIC %md # Databricksによる機械学習モデルの構築から予測実行まで
# MAGIC 
# MAGIC ※ 本 notebook は、Code ベースで ML シナリオをランスルーするノートブックになります。
# MAGIC 
# MAGIC ※ 本 notebook を Run All して作成されたデータセットは、後続 notebook (AutoML) でのデータセットとして活用頂けます。

# COMMAND ----------

# MAGIC %md ## デモ概要
# MAGIC databricksのテーブルに格納されているサンプルの与信データを使用し、貸し倒れ予測のモデルの作成からスコアリングまでの以下の流れをご紹介させていただきます。
# MAGIC 
# MAGIC - 1.トレーニングデータの加工
# MAGIC - 2.AutoML(自動チューニング＋分散モデルトレーニング)の実行
# MAGIC - 3.モデルレジストリへの登録とバージョン管理
# MAGIC - 4.モデルデプロイ(バッチ予測)

# COMMAND ----------

# MAGIC %md ## 1.トレーニングデータの準備
# MAGIC #####クラウドストレージ上のファイルからトレーニングデータ(ml_loan_status_workテーブル)を作成
# MAGIC databricksにテーブルとして格納されているサンプルデータ。
# MAGIC (USのLendingClubという会社が公開している与信データ)

# COMMAND ----------

import re
from pyspark.sql.types import * 
import pandas as pd
from pyspark.sql.types import StringType

# COMMAND ----------

# Username を取得。
username_raw = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().apply('user')
# Username の英数字以外を除去し、全て小文字化。Username をファイルパスやデータベース名の一部で使用可能にするため。
username = re.sub('[^A-Za-z0-9]+', '', username_raw).lower()

# データベース名を生成。
db_name = f"{username}" + '_db'

# パス指定 Sparkではdbfs:を使用してアクセス
table_path = '/user/hive/warehouse/' + username + '_db.db/' 

# 既存のデータを削除
# Parquest/Deltaテーブルのパスを削除。
# dbutils.fs.rm(table_path, True)

# データベースの準備
spark.sql(f"USE catalog hive_metastore")
#spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
spark.sql(f"USE {db_name}")

print("database  : " + db_name)
print("table_path:" + table_path)

# COMMAND ----------

# MAGIC %python
# MAGIC # パス指定 Sparkではdbfs:を使用してアクセス
# MAGIC source_path = 'dbfs:/databricks-datasets/samples/lending_club/parquet/'
# MAGIC 
# MAGIC # ソースディレクトリにあるParquetファイルをデータフレームに読み込む
# MAGIC data = spark.read.parquet(source_path)
# MAGIC 
# MAGIC # 読み込まれたデータを参照
# MAGIC display(data)
# MAGIC 
# MAGIC # レコード件数確認
# MAGIC print("レコード件数:" , data.count())

# COMMAND ----------

# MAGIC %python
# MAGIC # SQLで扱いやすいようにテンポラリビューとして登録
# MAGIC data.createOrReplaceTempView("bronze_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from bronze_data;

# COMMAND ----------

# DBTITLE 1,SQLによるデータクレンジング(不要なカラム/レコードの削除)
# MAGIC %sql 
# MAGIC -- クレンジングを実施します
# MAGIC DROP TABLE IF EXISTS loan_stats_work;
# MAGIC CREATE TABLE loan_stats_work AS
# MAGIC SELECT
# MAGIC   annual_inc,
# MAGIC   addr_state,
# MAGIC   chargeoff_within_12_mths,
# MAGIC   delinq_2yrs,
# MAGIC   delinq_amnt,
# MAGIC   dti,
# MAGIC   emp_title,
# MAGIC   grade,
# MAGIC   home_ownership,
# MAGIC   cast(replace(int_rate, '%', '') as float) as int_rate,
# MAGIC   installment,
# MAGIC   loan_amnt,
# MAGIC   open_acc,
# MAGIC   pub_rec,
# MAGIC   purpose,
# MAGIC   pub_rec_bankruptcies,
# MAGIC   revol_bal,
# MAGIC   cast(replace(revol_util, '%', '') as float) as revol_util,
# MAGIC   sub_grade,
# MAGIC   total_acc,
# MAGIC   verification_status,
# MAGIC   zip_code,
# MAGIC   case
# MAGIC     when loan_status = 'Fully Paid' then 0
# MAGIC     else 1
# MAGIC   end as bad_loan
# MAGIC FROM
# MAGIC   bronze_data
# MAGIC WHERE
# MAGIC   loan_status in (
# MAGIC     'Fully Paid',
# MAGIC     'Default',
# MAGIC     'Charged Off',
# MAGIC     'Late (31-120 days)',
# MAGIC     'Late (16-30 days)'
# MAGIC   )
# MAGIC   AND addr_state is not null
# MAGIC   AND annual_inc >= 30000;

# COMMAND ----------

# DBTITLE 1,テーブル定義の確認
# MAGIC %sql
# MAGIC describe extended loan_stats_work;

# COMMAND ----------

# DBTITLE 1,SQLによるデータの確認
# MAGIC %sql
# MAGIC select * from loan_stats_work;

# COMMAND ----------

# DBTITLE 1,データの可視化
# MAGIC %sql
# MAGIC --特定の特徴量が予測対象にどの程度影響しているかをBoxチャートで確認
# MAGIC select case when bad_loan = 1 then '貸し倒れ' else '完済' end as bad_loan , annual_inc as `年収` from loan_stats_work

# COMMAND ----------

# MAGIC %md 
# MAGIC ### SQLによるデータ加工

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL文レベルのACIDトランザクションサポートとデータ変更履歴管理機能

# COMMAND ----------

# MAGIC %sql
# MAGIC --(1) emp_titleの種類が多いので職業についている場合は"UNEMPLOYED"を代入
# MAGIC update loan_stats_work set emp_title = 'EMPLOYED' where emp_title is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC --(2) emp_titleの欠損値には"UNEMPLOYED"を代入
# MAGIC update loan_stats_work set emp_title = 'UNEMPLOYED' where emp_title is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan_stats_work;

# COMMAND ----------

# トレーニングデータをSparkデータフレームに格納
loan_stats_df = spark.sql("select * from loan_stats_work")

# COMMAND ----------

#ここではrandomSplit()を使って、短時間でモデル生成が終了するようデモでは5%のサンプルを使用してトレーニングを実行
(train_df, test_df) = loan_stats_df.randomSplit([0.05, 0.95], seed=123)

# COMMAND ----------

print("トレーニングデータ件数:" , train_df.count())
print("テストデータ件数:" , test_df.count())

# COMMAND ----------

# MAGIC %python
# MAGIC # トレーニングデータをテーブルとして登録しておく
# MAGIC spark.sql("drop table if exists loan_stats_train")
# MAGIC train_df.write.saveAsTable("loan_stats_train")
# MAGIC 
# MAGIC # テストデータは後ほどSQL関数にデプロイしたモデルで予測で使用するため、予測対象カラム(bad_loan)を除外してから、テーブルとして登録しておく
# MAGIC spark.sql("drop table if exists loan_stats_test")
# MAGIC prediction_data_df = test_df.drop("bad_loan")
# MAGIC prediction_data_df.write.saveAsTable("loan_stats_test")
# MAGIC 
# MAGIC # DelteLiveTableとの連携デモ用に空の入力テーブルも作成しておく(このNotebookでは使用しません)
# MAGIC spark.sql("drop table if exists loan_stats_input")
# MAGIC spark.sql("create table if not exists loan_stats_input as select current_timestamp() as ingest_time , * from loan_stats_test limit 0")

# COMMAND ----------

# MAGIC %md モデルを作成する準備ができました。

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2.AutoML(ハイパーパラメータ・チューニング＋分散学習の実行)
# MAGIC 
# MAGIC 次のコードでは、AutoMLを使用してモデルの分散トレーニングを実行し、同時にハイパーパラメータのチューニングを実行します。</br>
# MAGIC またMLflowにより、学習に使用されたハイパーパラメータ設定とそれぞれの精度をトラッキングし、モデルアーチファクトとともに保存します。</br>
# MAGIC https://docs.microsoft.com/en-us/azure/databricks/applications/machine-learning/automl</br>
# MAGIC </br>
# MAGIC Databricks Runtime 11.0 ML以降の場合：</br>
# MAGIC 
# MAGIC コアあたりのメモリが多いワーカーノードでは、サンプリングの割合が増加します。メモリ最適化インスタンスタイプを選択することにより、サンプルサイズを増やすことができます。</br>
# MAGIC spark.task.cpusクラスターのSpark構成でより大きな値を選択することにより、サンプルサイズをさらに増やすことができます。デフォルト設定は1です。最大値は、ワーカーノード上のCPUの数です。</br>
# MAGIC この値を大きくすると、サンプルサイズは大きくなりますが、並行して実行される試行は少なくなります。<br>
# MAGIC たとえば、4コアで合計64GBのRAMを搭載したマシンでは、デフォルトspark.task.cpus=1でワーカーごとに4つのトライアルが実行され、各トライアルは16GBのRAMに制限されます。</br>
# MAGIC spark.task.cpus=4を設定すると、各ワーカーは1つのトライアルのみを実行しますが、そのトライアルは64GBのRAMを使用できます。</br>
# MAGIC AutoMLがデータセットをサンプリングした場合、サンプリングの割合はUIの[概要]タブに表示されます。</br>

# COMMAND ----------

from databricks import automl
summary = automl.classify(train_df, target_col="bad_loan",primary_metric="roc_auc", timeout_minutes=5)

# COMMAND ----------

# 実験IDの取得
print("experiment_id=", summary.experiment.experiment_id)

# COMMAND ----------

# MAGIC %md
# MAGIC MLFlowが持つオートロギングの設定で(1)ハイパーパラメータ (2)作成されたモデル (2)特徴量の重要度の３つはモデルアーチファクトとして自動で保存されます。

# COMMAND ----------

# MAGIC %md  
# MAGIC #### MLflowのUIで結果を参照してみる
# MAGIC 
# MAGIC MLflow の実行結果を見るには、Experiment Runs サイドバーを開きます。下向き矢印の隣の日付(Date)をクリックしてメニューを表示し、「auc」を選択して、auc メトリックでソートされた実行を表示します。
# MAGIC 
# MAGIC MLflow は、各ランのパラメータとパフォーマンス指標を追跡します。Experiment Runs サイドバーの上部にある外部リンクアイコン <img src="https://docs.microsoft.com/azure/databricks/_static/images/external-link.png"/> をクリックすると、MLflow のランテーブルに移動します。

# COMMAND ----------

# MAGIC %md ## 3.モデルレジストリへの登録とステータス管理

# COMMAND ----------

# DBTITLE 0,モデリング結果の取得
# モデリング結果の取得
# MLfow関連
import mlflow
import mlflow.pyfunc
import mlflow.xgboost
from mlflow.models.signature import infer_signature

# APIでも最も精度の高いモデルの情報を取得できます。
best_run = mlflow.search_runs(experiment_ids=summary.experiment.experiment_id,order_by=["metrics.val_roc_auc_score DESC"],  max_results=1)
display(best_run)
print(f'Best Run ID : {best_run.run_id}')
print(f'AUC of Best Run AUC: {best_run["metrics.val_roc_auc_score"]}')

# COMMAND ----------

# MAGIC %md ### 最も精度の高いモデルをモデルレジストリに登録

# COMMAND ----------

#モデルレジストリに登録
# 最も精度の高いモデルを任意の名前をつけて登録
model_name = "DEMO_000" + username
# run_idをテキスト文字列に変換
run_id_text = ''.join(best_run.run_id)

# モデルURL
logged_model = f"runs:/{run_id_text}/model"
print(logged_model)

# モデルレジストリに名前をつけて登録
new_model_version = mlflow.register_model(logged_model, model_name)

# COMMAND ----------

# MAGIC %md ### モデルのステータス変更(ステージング ---> プロダクション)

# COMMAND ----------

#APIでもモデルのステータス変更が可能です。
#MLflowに接続
from mlflow.tracking import MlflowClient
client = MlflowClient()

# 新しいモデルをProductionへ昇格
client.transition_model_version_stage(
  name=model_name,
  version=new_model_version.version,
  stage="Production",
)

# COMMAND ----------

# MAGIC %md ## 4.予測の実行
# MAGIC 
# MAGIC ここでは予測実行ユーザとしてプロダクションとなっているモデルを使用して予測を実行するデモをお見せします。
# MAGIC 予測デモでは先ほどのトレーニングデータを予測対象データとして使用することとします。

# COMMAND ----------

# MAGIC %md ### 最初にプロダクションとして利用可能なモデルのダウンロード

# COMMAND ----------

# 予測に使用するモデルを名前指定でダウンロード
model_name = "DEMO_000" + username

# #load_model関数で、クライアントはモデルレジストリから指定したモデルを受け取ることができます。
model = mlflow.pyfunc.load_model(f"models:/{model_name}/production")
print("--Metadata--")
print(model.metadata)

# COMMAND ----------

# MAGIC %md ### 予測対象データの準備

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from loan_stats_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from loan_stats_test;

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQLでの予測実行
# MAGIC 
# MAGIC 以下のコードは、モデルをSQL関数として登録し、Deltaテーブルに格納されたデータに対して分散スコアリングを実行しています。

# COMMAND ----------

#ダウンロードしたモデルをUDFとして実装
import mlflow.pyfunc
from pyspark.sql.functions import struct

#　モデルをモデルレジストリからダウンロード
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# 作成したモデルをSQL関数として登録
spark.udf.register('loan_score', loaded_model ) 

#引数として使用するためカラム名のリストを取得
udf_inputs = struct(*(prediction_data_df.toPandas().columns.tolist()))
print(udf_inputs)

# COMMAND ----------

# DBTITLE 1,SQL関数でモデルを実行
# MAGIC %sql
# MAGIC select
# MAGIC   annual_inc,
# MAGIC   addr_state,
# MAGIC   chargeoff_within_12_mths,
# MAGIC   delinq_2yrs,
# MAGIC   delinq_amnt,
# MAGIC   dti,
# MAGIC   emp_title,
# MAGIC   grade,
# MAGIC   home_ownership,
# MAGIC   int_rate,
# MAGIC   installment,
# MAGIC   loan_amnt,
# MAGIC   open_acc,
# MAGIC   pub_rec,
# MAGIC   purpose,
# MAGIC   pub_rec_bankruptcies,
# MAGIC   revol_bal,
# MAGIC   revol_util,
# MAGIC   sub_grade,
# MAGIC   total_acc,
# MAGIC   verification_status,
# MAGIC   zip_code,
# MAGIC   -- 以下は作成したモデルを実装したSQL関数です
# MAGIC   loan_score(
# MAGIC     annual_inc,
# MAGIC     addr_state,
# MAGIC     chargeoff_within_12_mths,
# MAGIC     delinq_2yrs,
# MAGIC     delinq_amnt,
# MAGIC     dti,
# MAGIC     emp_title,
# MAGIC     grade,
# MAGIC     home_ownership,
# MAGIC     int_rate,
# MAGIC     installment,
# MAGIC     loan_amnt,
# MAGIC     open_acc,
# MAGIC     pub_rec,
# MAGIC     purpose,
# MAGIC     pub_rec_bankruptcies,
# MAGIC     revol_bal,
# MAGIC     revol_util,
# MAGIC     sub_grade,
# MAGIC     total_acc,
# MAGIC     verification_status,
# MAGIC     zip_code
# MAGIC   ) as predict_bad_loan
# MAGIC from
# MAGIC   loan_stats_test;

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <head>
# MAGIC   <link href="https://fonts.googleapis.com/css2?family=Kosugi+Maru&display=swap" rel="stylesheet">
# MAGIC   <style>
# MAGIC     h1,h2,h3,p,span,td, div {font-family: "Kosugi Maru", sans-serif !important;}
# MAGIC   </style>
# MAGIC </head>
# MAGIC 
# MAGIC <h1>END</h1>  
