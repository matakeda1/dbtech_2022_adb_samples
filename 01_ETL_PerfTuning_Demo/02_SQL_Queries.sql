-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## 性能改善 検証クエリ
-- MAGIC 
-- MAGIC 以下クエリを2つコピーし、SQL タブに切替えて SQL Warehouse で実行してください
-- MAGIC ![image](https://user-images.githubusercontent.com/38490168/201463434-44feb94f-d4c0-4654-b0b2-c9ab59c5d929.png)

-- COMMAND ----------

use <データベース名>;
set use_cached_result = false;
set ENABLE_PHOTON = false;

-- Parquetフォーマット（標準Sparkコンフィグ)

SELECT /* parquet */
     l_orderkey,
     SUM(l_extendedprice * (1 - l_discount)) AS revenue,
     o_orderdate,
     o_shippriority
 FROM customer_parqt, orders_parqt, lineitem_parqt
WHERE
      c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_shipmode = 'AIR'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC,  o_orderdate
limit 100;

-- COMMAND ----------

use <データベース名>;
set use_cached_result = true;
set ENABLE_PHOTON = true;

-- Deltaフォーマット(Azure Databricks: Photonエンジン + オプティマイズ)

SELECT /* delta */
     l_orderkey,
     SUM(l_extendedprice * (1 - l_discount)) AS revenue,
     o_orderdate,
     o_shippriority
 FROM  customer_delta, orders_delta, lineitem_delta
WHERE
      c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND l_shipmode = 'AIR'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC,  o_orderdate
limit 100;
