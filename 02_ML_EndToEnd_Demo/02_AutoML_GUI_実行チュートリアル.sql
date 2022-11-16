-- Databricks notebook source
-- MAGIC %md
-- MAGIC # AutoML UI 実行手順

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 参考ドキュメント
-- MAGIC 
-- MAGIC https://learn.microsoft.com/ja-jp/azure/databricks/machine-learning/automl/train-ml-model-automl-ui

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. AutoML エクスペリメント設定
-- MAGIC 
-- MAGIC 以下設定を確認し AutoML を実行しましょう。AutoML が終わった後は、最も精度の良いモデルをモデル登録し、その登録したモデルのIDをメモしましょう

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201459389-973ad359-4084-4fd6-a3c7-a120e654d32d.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. モデル登録
-- MAGIC 
-- MAGIC AutoML 実行結果の UI から対象のモデルを選択すると、以下図 (右側中央) のように、モデル登録ボタンが表示されますので、選択しましょう

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201459467-e7e45d37-0d7d-4d88-9adc-8f69493349e0.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. モデル名の取得 
-- MAGIC 
-- MAGIC モデル登録が完了したら、登録したモデル名をメモしておきましょう。後続の推論ワークロードで指定するパラメータになります。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201459651-a036710c-5ff6-44c1-afb0-2c423ad04d51.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 推論パイプラインの実行
-- MAGIC 
-- MAGIC 同ディレクトリに配置された "03_Predict_(DLTInference_Streaming)" ノートブックを、環境に合わせてパラメータ設定および実行してください。
-- MAGIC 
-- MAGIC (上記でメモをしたモデル名、及びデータベース名が必要になります。)

-- COMMAND ----------


