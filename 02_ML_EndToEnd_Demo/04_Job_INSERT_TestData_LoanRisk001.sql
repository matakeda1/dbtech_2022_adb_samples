-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## サンプルクエリ
-- MAGIC 
-- MAGIC 以下コマンドは、本ノートブックでは実行しないでください。SQL タブに切替え、SQLエディタに貼り付けて実行しましょう

-- COMMAND ----------

SET timezone = +9:00;
use <データベース名>;

-- 新規の融資申し込みがあったことをイメージしています。INSERT 文によって新規データの追加を実行できます。
-- SQL クエリはスケジュール実行が出来るため、スケジューリングによって、毎分おきに実行することも可能です。
-- truncate table loan_stats_input;

insert into loan_stats_input 
select * except(rowid)
FROM (
select 
row_number() over (order by RAND()) as rowid,
current_timestamp(), * 
from loan_stats_test 
) 
-- スコアリング対象レコード数はランダムとなるよう入力テーブルにインサートする
where rowid <= cast(substr(cast(current_timestamp() as char(20)) , 21,3) as int) / 5 + 50

-- 確認
-- select * from dbtech_model_predictions;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## (補足) スケジュール実行
-- MAGIC 
-- MAGIC 本コマンドを、"ワークフロー" タブからスケジュール実行の設定が可能です

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201466289-401d1b9e-b8d7-4448-a834-101563b4ab4e.png)

-- COMMAND ----------


