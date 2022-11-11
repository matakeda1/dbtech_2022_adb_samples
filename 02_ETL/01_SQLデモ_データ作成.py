# Databricks notebook source
# MAGIC %md-sandbox 
# MAGIC # 高信頼性と高パフォーマンスを実現するDelta Lakeを体験
# MAGIC 
# MAGIC <img style="margin-top:25px;" src="https://jixjiadatabricks.blob.core.windows.net/images/databricks-logo-small-new.png" width="140">
# MAGIC <hr>
# MAGIC <h3>データレイクに<span style="color='#38a'">信頼性</span>と<span style="color='#38a'">パフォーマンス</span>をもたらす</h3>
# MAGIC <p>本編はデモデータを使用してDelta Lakeが提供する主要な機能に関して説明していきます。</p>
# MAGIC <div style="float:left; padding-right:60px; margin-top:20px; margin-bottom:200px;">
# MAGIC   <img src="https://jixjiadatabricks.blob.core.windows.net/images/delta-lake-square-black.jpg" width="220">
# MAGIC </div>
# MAGIC 
# MAGIC <div style="float:left; margin-top:0px; padding:0;">
# MAGIC   <h3>信頼性</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>DMLサポート（INSERTだけではなくUPDATE/DELETE/MERGEをサポート）</li>
# MAGIC     <li>データ品質管理　(スキーマ・エンフォース/エボリューション)</li>
# MAGIC     <li>トランザクションログによるACIDコンプライアンスとタイムトラベル (データのバージョン管理)</li>
# MAGIC    </ul>
# MAGIC 
# MAGIC   <h3>パフォーマンス</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>Photonエンジン</li>
# MAGIC     <li>Optimizeによるコンパクションとデータスキッピング</li>
# MAGIC     <li>Deltaキャッシング</li>
# MAGIC   </ul>
# MAGIC 
# MAGIC  <h3>バッチデータとストリーミングデータの統合</h3>
# MAGIC   <ul style="padding-left: 30px;">
# MAGIC     <li>ストリーミングデータの取り込み</li>
# MAGIC     <li>ストリーミングデータのリアルタイムETL処理</li>
# MAGIC     <li>バッチデータとストリーミングデータに対する分析処理</li>
# MAGIC    </ul>
# MAGIC   </div>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Parquet + トランザクションログ
# MAGIC <p  style="position:relative;width: 500px;height: 450px;overflow: hidden;">
# MAGIC <img style="margin-top:25px;" src="https://sajpstorage.blob.core.windows.net/itagaki/delta1.png" width="1000">
# MAGIC </p>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.共通データ準備
# MAGIC 
# MAGIC 今回使用するデータはDatabricksのデモデータとしてストレージに格納されているTPCHのデータセットを使用します。</br>
# MAGIC 
# MAGIC 2台のワーカー (D16as_v5)メモリ64GB、コア16個 * 2　= メモリ128GB 32 コア </br>
# MAGIC 1台のドライバー メモリ64GB、コア16個 </br>
# MAGIC ランタイム 11.3.x-scala2.12

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

dbutils.fs.ls("dbfs:/databricks-datasets/tpch/delta-001/")

# COMMAND ----------

# MAGIC %sql -- 付属のデモデータに対して外部表を作成
# MAGIC drop table if exists lineitem_ext;
# MAGIC create external table lineitem_ext USING DELTA LOCATION 'dbfs:/databricks-datasets/tpch/delta-001/lineitem';

# COMMAND ----------

# MAGIC %sql -- 付属のデモデータに対して外部表を作成
# MAGIC drop table if exists  customer_ext;
# MAGIC create external table customer_ext USING DELTA LOCATION 'dbfs:/databricks-datasets/tpch/delta-001/customer';

# COMMAND ----------

# MAGIC %sql -- 付属のデモデータに対して外部表を作成
# MAGIC drop table if exists  orders_ext;
# MAGIC create external table orders_ext USING DELTA LOCATION 'dbfs:/databricks-datasets/tpch/delta-001/orders';

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_ext;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from orders_ext;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from lineitem_ext;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.Parquetフォーマットのデータ準備

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists customer_parqt;
# MAGIC create table customer_parqt using parquet AS select * from customer_ext order by c_custkey

# COMMAND ----------

# データ増幅用のクエリ
sqltext1 = "create table orders_parqt using parquet AS select * from orders_ext order by o_orderkey"
sqltext2 = "insert into orders_parqt select * from orders_ext order by o_orderkey"

################
# データ増幅の開始
################
#　　3分ほどかかるのでこの間に簡単にDeltaを説明します。
#　　テーブル作成
sql("drop table if exists orders_parqt")
print(1,sqltext1)
sql(sqltext1)
# データ増幅
print(2,sqltext2)
sql(sqltext2)

print(3,sqltext2)
sql(sqltext2)

print(4,sqltext2)
sql(sqltext2)

print(5,sqltext2)
sql(sqltext2)

print(6,sqltext2)
sql(sqltext2)

print(7,sqltext2)
sql(sqltext2)

print(8,sqltext2)
sql(sqltext2)

print(9,sqltext2)
sql(sqltext2)

print(10,sqltext2)
sql(sqltext2)

#print(11,sqltext2)
#sql(sqltext2)

#print(12,sqltext2)
#sql(sqltext2)

#print(13,sqltext2)
#sql(sqltext2)

#print(14,sqltext2)
#sql(sqltext2)

#print(15,sqltext2)
#sql(sqltext2)

# COMMAND ----------

# データ増幅用のクエリ
sqltext1 = "create table lineitem_parqt using parquet AS select * from lineitem_ext order by l_orderkey"
sqltext2 = "insert into lineitem_parqt select *  from lineitem_ext order by l_orderkey"

################
# データ増幅の開始
################
#　　3分ほどかかるのでこの間に簡単にDeltaを説明します。
#　　テーブル作成
sql("drop table if exists lineitem_parqt")
print(1,sqltext1)
sql(sqltext1)
# データ増幅
print(2,sqltext2)
sql(sqltext2)

print(3,sqltext2)
sql(sqltext2)

print(4,sqltext2)
sql(sqltext2)

print(5,sqltext2)
sql(sqltext2)

print(6,sqltext2)
sql(sqltext2)

print(7,sqltext2)
sql(sqltext2)

print(8,sqltext2)
sql(sqltext2)

print(9,sqltext2)
sql(sqltext2)

print(10,sqltext2)
sql(sqltext2)

#print(11,sqltext2)
#sql(sqltext2)

#print(12,sqltext2)
#sql(sqltext2)

#print(13,sqltext2)
#sql(sqltext2)

#print(14,sqltext2)
#sql(sqltext2)

#print(15,sqltext2)
#sql(sqltext2)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.io.cache.enabled = false;
# MAGIC SET spark.databricks.photon.enabled = false;
# MAGIC 
# MAGIC SELECT /* parquet */
# MAGIC      l_orderkey,
# MAGIC      SUM(l_extendedprice * (1 - l_discount)) AS revenue,
# MAGIC      o_orderdate,
# MAGIC      o_shippriority,
# MAGIC      count(1) as cnt
# MAGIC  FROM customer_parqt, orders_parqt, lineitem_parqt
# MAGIC WHERE c_mktsegment = 'BUILDING'
# MAGIC   AND c_custkey = o_custkey
# MAGIC   AND l_orderkey = o_orderkey
# MAGIC   AND o_orderdate < '1995-03-15'
# MAGIC   AND l_shipdate > '1995-03-15'
# MAGIC   AND l_shipmode = 'AIR'
# MAGIC GROUP BY l_orderkey, o_orderdate, o_shippriority
# MAGIC ORDER BY revenue DESC,  o_orderdate
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.Deltaフォーマットのデータ準備

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists customer_delta;
# MAGIC create table customer_delta AS select * from customer_ext order by c_custkey

# COMMAND ----------

# データ増幅用のクエリ
sqltext1 = "create table orders_delta AS select * from orders_ext order by o_orderkey"
sqltext2 = "insert into orders_delta select * from orders_ext order by o_orderkey"

################
# データ増幅の開始
################
#　　3分ほどかかるのでこの間に簡単にDeltaを説明します。
#　　テーブル作成
sql("drop table if exists orders_delta")
print(1,sqltext1)
sql(sqltext1)
# データ増幅
print(2,sqltext2)
sql(sqltext2)

print(3,sqltext2)
sql(sqltext2)

print(4,sqltext2)
sql(sqltext2)

print(5,sqltext2)
sql(sqltext2)

print(6,sqltext2)
sql(sqltext2)

print(7,sqltext2)
sql(sqltext2)

print(8,sqltext2)
sql(sqltext2)

print(9,sqltext2)
sql(sqltext2)

print(10,sqltext2)
sql(sqltext2)

#print(11,sqltext2)
#sql(sqltext2)

#print(12,sqltext2)
#sql(sqltext2)

#print(13,sqltext2)
#sql(sqltext2)

#print(14,sqltext2)
#sql(sqltext2)

#print(15,sqltext2)
#sql(sqltext2)

# COMMAND ----------

# MAGIC %python
# MAGIC 
# MAGIC # データ増幅用のクエリ
# MAGIC sqltext1 = "create table lineitem_delta  AS select * from lineitem_ext order by l_orderkey"
# MAGIC sqltext2 = "insert into lineitem_delta select *  from lineitem_ext order by l_orderkey"
# MAGIC 
# MAGIC ################
# MAGIC # データ増幅の開始
# MAGIC ################
# MAGIC #　　3分ほどかかるのでこの間に簡単にDeltaを説明します。
# MAGIC #　　テーブル作成
# MAGIC sql("drop table if exists lineitem_delta")
# MAGIC print(1,sqltext1)
# MAGIC sql(sqltext1)
# MAGIC # データ増幅
# MAGIC print(2,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(3,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(4,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(5,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(6,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(7,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(8,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(9,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC print(10,sqltext2)
# MAGIC sql(sqltext2)
# MAGIC 
# MAGIC #print(11,sqltext2)
# MAGIC #sql(sqltext2)
# MAGIC 
# MAGIC #print(12,sqltext2)
# MAGIC #sql(sqltext2)
# MAGIC 
# MAGIC #print(13,sqltext2)
# MAGIC #sql(sqltext2)
# MAGIC 
# MAGIC #print(14,sqltext2)
# MAGIC #sql(sqltext2)
# MAGIC 
# MAGIC #print(15,sqltext2)
# MAGIC #sql(sqltext2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.Deltaフォーマットで可能なチューニング

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize customer_delta zorder by (c_mktsegment);

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize orders_delta zorder by (o_orderdate);

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize lineitem_delta zorder by (l_shipdate,l_shipmode);

# COMMAND ----------

# MAGIC %sql
# MAGIC analyze table lineitem_delta compute statistics;
# MAGIC analyze table orders_delta compute statistics;
# MAGIC analyze table customer_delta compute statistics;

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.io.cache.enabled = true;
# MAGIC SET spark.databricks.photon.enabled = true;
# MAGIC 
# MAGIC SELECT /* delta */
# MAGIC      l_orderkey,
# MAGIC      SUM(l_extendedprice * (1 - l_discount)) AS revenue,
# MAGIC      o_orderdate,
# MAGIC      o_shippriority,
# MAGIC      count(1) as cnt
# MAGIC  FROM  customer_delta, orders_delta, lineitem_delta
# MAGIC WHERE c_mktsegment = 'BUILDING'
# MAGIC   AND c_custkey = o_custkey
# MAGIC   AND l_orderkey = o_orderkey
# MAGIC   AND o_orderdate < '1995-03-15'
# MAGIC   AND l_shipdate > '1995-03-15'
# MAGIC   AND l_shipmode = 'AIR'
# MAGIC GROUP BY l_orderkey, o_orderdate, o_shippriority
# MAGIC ORDER BY revenue DESC,  o_orderdate
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.レコード件数チェック

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from customer_parqt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from customer_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from orders_parqt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from orders_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from lineitem_parqt;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from lineitem_delta;
