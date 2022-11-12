-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Databricks Dashboard 作成手順
-- MAGIC 
-- MAGIC 前述 AutoML / モデル登録 / リアルタイム推論のワークロードで作成されるデータを基に、ダッシュボードを作成します。
-- MAGIC 
-- MAGIC 本MLワークロードのシナリオは、信用リスク分類モデルです。従って、本可視化のダッシュボードでは、信用MLモデルによって判定された結果をダッシュボードでリアルタイムに状況モニタリングする、簡易な例を表現しています。
-- MAGIC 
-- MAGIC 以下はアウトプットのイメージです。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201461137-171f4cf3-fafa-4ee2-ae38-84d2542bfddd.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. クエリ作成
-- MAGIC 
-- MAGIC 以下のファイル名で、計4つのクエリをコピーペーストし、各々のクエリを保存してください。
-- MAGIC 
-- MAGIC SQL タブの SQLエディタにて、以下のように作成・保存ができます。
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201462737-1b5d73de-0d55-406a-bd06-8e091260a96f.png)

-- COMMAND ----------

-- ファイル名: Query_LoanRisk_Prediction001
-- クエリ内容: 推論結果の全件表示
-- 設定内容: LIMIT 1000 の解除、データベース名の設定

select
  *
from
  dbtech_model_predictions;

-- COMMAND ----------

-- ファイル名: Query_LoanRisk_Prediction002
-- クエリ内容: 推論結果のうち、融資 NG のみ全件表示
-- 設定内容: LIMIT 1000 の解除、データベース名の設定

select
  *
from
  dbtech_model_predictions
where
  prediction = 1;

-- COMMAND ----------

-- ファイル名: Query_LoanRisk_Prediction003
-- クエリ内容: 推論結果のうち、融資 OK のみ全件表示
-- 設定内容: LIMIT 1000 の解除、データベース名の設定

select
  *
from
  dbtech_model_predictions
where
  prediction = 0;

-- COMMAND ----------

-- ファイル名: Query_LoanRisk_Prediction004
-- クエリ内容: 推論結果のうち、融資 NG 比率を計算
-- 設定内容: LIMIT 1000 の解除、データベース名の設定

SELECT
  concat(
    round(
      (
        sum(
          case
            prediction
            WHEN 1 THEN 1
            ELSE 0
          END
        ) / count(*) * 100
      ),
      1
    ),
    '%'
  ) as `融資NG比率`
FROM
  dbtech_model_predictions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. 可視化の作成
-- MAGIC 
-- MAGIC 前述で作成した4つのクエリにおいて各々、各可視化を作成します。設定後のイメージは、以下をご参照下さい。
-- MAGIC 
-- MAGIC ご自身の可視化したい表現に、適宜変更頂いて問題ありません。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query_LoanRisk_Prediction001
-- MAGIC * 作成する可視化表現
-- MAGIC     * リアルタイム信用予測モニタリング
-- MAGIC     * 州別融資 NG 割合 (%)
-- MAGIC     * 住宅別融資 NG 割合 (%)
-- MAGIC     * 雇用形態別融資 NG 割合 (%)
-- MAGIC     * 目的別 融資 NG 割合 (%)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201462793-1e274595-62e9-4c1c-b671-ba39e2f22fa0.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201462832-69e485b3-b6f9-4960-bae0-6d8c1c0e6b47.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201462866-daf05ab0-f017-46ad-bf77-f5a9152d0f56.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201462920-15fb4018-dd31-40e7-9051-06d91bd30365.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201462940-13056bd2-77b9-4f3e-b612-e8db8f3557b2.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query_LoanRisk_Prediction002
-- MAGIC * 作成する可視化表現
-- MAGIC     * 融資 NG カウント数
-- MAGIC     * 融資 NG 申込一覧
-- MAGIC     * 融資 NG 件数ヒートマップ

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463077-90320ca8-9463-4053-b701-15cb9a2aa942.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463122-90354d3b-0136-42e0-ad99-dd6892f52efa.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463137-bdf571f2-e7f5-447e-8ef5-b4af96e3bd48.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query_LoanRisk_Prediction003
-- MAGIC * 作成する可視化表現
-- MAGIC     * 融資 OK カウント数

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463176-2b69f909-f6af-4358-a733-8cf2372385f7.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Query_LoanRisk_Prediction004
-- MAGIC * 作成する可視化表現
-- MAGIC     * 融資 NG 比率

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463207-fecf7574-ebea-4f1e-9d76-e73cefaf5ff4.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 3. ダッシュボードの作成
-- MAGIC 
-- MAGIC 上記で作業していた SQLエディタから、シームレスにダッシュボードの作成が可能です。作成した可視化の1つを選択し、"ダッシュボードに追加" を選択しましょう。
-- MAGIC 
-- MAGIC 最初は、新規のダッシュボードを作成してから可視化を張り付けるようという手順になります。以降、他の可視化については、最初に作成したダッシュボードに貼り付け、最終的には本 notebook の最初に掲示されているようなダッシュボードを作成しましょう。　　

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * 特定の可視化を選択し、ダッシュボードを追加 (最初は、ダッシュボードの新規作成)
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463245-e1dd3196-df13-4dd3-950d-87488fe0a4e7.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * 新規ダッシュボードの作成
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463347-0aa3d30f-22b4-4385-8ac0-407d2ac31f40.png)

-- COMMAND ----------


