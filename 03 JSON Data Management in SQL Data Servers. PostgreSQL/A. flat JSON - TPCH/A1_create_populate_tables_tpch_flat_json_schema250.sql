----------------------------------------------------------
--  Given a TPC-H database schema and its content for the
--  scale factor of 0.01, we'll create and populate
--  a denormalized schema (schema no 250) in which each
-- table (normalized or denormalized) containts a single
-- JSON attribute
----------------------------------------------------------
-- for details about TPC-H benchmark - see the pdf file:
https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/03%20JSON%20Data%20Management%20in%20SQL%20Data%20Servers.%20PostgreSQL/A.%20flat%20JSON%20-%20TPCH/tpc-h_v2.18.0.pdf


----------------------------------------------------------

-- Step. 1: download the backup file for TPC-H schema (sf 0.01):
https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/03%20JSON%20Data%20Management%20in%20SQL%20Data%20Servers.%20PostgreSQL/A.%20flat%20JSON%20-%20TPCH/tpch0_01.backup


-- Step. 2: on your laptop, create a PostgreSQL database called `tpch0_01`


-- Step. 3: Get the db content from the backup file (downloaded in step 1) using
--    `Restore` option in PgAdmin 4


-- Step 4. Set `tpch0_01` as the current database; open the `Query tool...`
-- Copy the commands below into the pgAdmin Query Tool anl lauch them:

 DROP SCHEMA IF EXISTS sf_0_01__schema_250__flat_JSON CASCADE ;
 CREATE SCHEMA sf_0_01__schema_250__flat_JSON;
-- path1: lineitem_orders_customer-nation-region
-- path2: lineitem-partsupp-part
-- path3: lineitem-partsupp_supplier_nation-region


CREATE TABLE sf_0_01__schema_250__flat_JSON.lineitem_orders_customer AS
SELECT to_jsonb(x) AS  lineitem_orders_customer  FROM ( SELECT * FROM  lineitem INNER JOIN orders ON lineitem.l_orderkey = orders.o_orderkey INNER JOIN customer ON orders.o_custkey = customer.c_custkey ) x ;

CREATE TABLE sf_0_01__schema_250__flat_JSON.nation AS
SELECT to_jsonb(x) AS  nation  FROM ( SELECT * FROM  nation ) x ;

CREATE TABLE sf_0_01__schema_250__flat_JSON.region AS
SELECT to_jsonb(x) AS  region  FROM ( SELECT * FROM  region ) x ;

CREATE TABLE sf_0_01__schema_250__flat_JSON.lineitem AS
SELECT to_jsonb(x) AS  lineitem  FROM ( SELECT * FROM  lineitem ) x ;

CREATE TABLE sf_0_01__schema_250__flat_JSON.partsupp AS
SELECT to_jsonb(x) AS  partsupp  FROM ( SELECT * FROM  partsupp ) x ;

CREATE TABLE sf_0_01__schema_250__flat_JSON.part AS
SELECT to_jsonb(x) AS  part  FROM ( SELECT * FROM  part ) x ;

CREATE TABLE sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation AS
SELECT to_jsonb(x) AS  partsupp_supplier_nation  FROM ( SELECT * FROM  partsupp INNER JOIN supplier ON partsupp.ps_suppkey = supplier.s_suppkey INNER JOIN nation ON supplier.s_nationkey = nation.n_nationkey ) x ;


-- There is no PRIMARY KEY option for JSON attributes...
--...but we can use indexes...
CREATE INDEX idx__lineitem__json ON sf_0_01__schema_250__flat_JSON.lineitem USING GIN (lineitem) ;

CREATE INDEX idx__lineitem_orders_customer__json ON sf_0_01__schema_250__flat_JSON.lineitem_orders_customer USING GIN (lineitem_orders_customer) ;

CREATE INDEX idx__nation__json ON sf_0_01__schema_250__flat_JSON.nation USING GIN (nation) ;

CREATE INDEX idx__part__json ON sf_0_01__schema_250__flat_JSON.part USING GIN (part) ;

CREATE INDEX idx__partsupp__json ON sf_0_01__schema_250__flat_JSON.partsupp USING GIN (partsupp) ;

CREATE INDEX idx__partsupp_supplier_nation__json ON sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation USING GIN (partsupp_supplier_nation) ;

CREATE INDEX idx__region__json ON sf_0_01__schema_250__flat_JSON.region USING GIN (region) ;


-- ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... REFERENCES ... (...)
-- NO FOREIGN KEYS AMONG JSON attributes!
