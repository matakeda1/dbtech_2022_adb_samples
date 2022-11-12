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

insert into loan_stats_input select current_timestamp(), * 
from loan_stats_test limit 150

-- 確認
-- select * from dbtech_model_predictions;
