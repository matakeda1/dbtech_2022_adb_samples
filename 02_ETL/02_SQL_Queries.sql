-- Databricks notebook source
use <データベース名>;
set use_cached_result = false;
set ENABLE_PHOTON = false;

SELECT /* parquet */
     l_orderkey,
     SUM(l_extendedprice * (1 - l_discount)) AS revenue,
     o_orderdate,
     o_shippriority,
     count(1) as cnt
 FROM customer_parqt, orders_parqt, lineitem_parqt
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
  AND l_shipmode = 'AIR'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC,  o_orderdate
LIMIT 10;

-- COMMAND ----------

use <データベース名>;
set use_cached_result = true;
set ENABLE_PHOTON = true;

SELECT /* delta */
     l_orderkey,
     SUM(l_extendedprice * (1 - l_discount)) AS revenue,
     o_orderdate,
     o_shippriority,
     count(1) as cnt
 FROM  customer_delta, orders_delta, lineitem_delta
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < '1995-03-15'
  AND l_shipdate > '1995-03-15'
  AND l_shipmode = 'AIR'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC,  o_orderdate
LIMIT 10;
