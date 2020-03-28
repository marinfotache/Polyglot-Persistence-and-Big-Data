----------------------------------------------------------
--  Given a TPC-H database schema and its content for the
--  scale factor of 0.01, we'll create and populate
--  a denormalized schema (schema no 250) in which each
-- table (normalized or denormalized) containts a single
-- JSON attribute
----------------------------------------------------------

-- 



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


__________________________________________________________________
-- extract all content of a table containing only a JSON attribute
SELECT *
FROM sf_0_01__schema_250__flat_JSON.nation ;


__________________________________________________________________
-- check the table structure in the original (normalized) TPC-H schema
select * from nation


__________________________________________________________________
-- extract JSON (sub)attributes as text
SELECT *,
	nation ->> 'n_nationkey' AS n_nationkey,  -- the data type is text here!
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation ;

__________________________________________________________________
-- cast needed for preserving the data type
SELECT *,
	cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,  -- the data type is ok now
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation ;


__________________________________________________________________
-- get, as a tabular result, the nations whose names starts with `R`
SELECT  cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation
WHERE (nation ->> 'n_name') LIKE 'R%' ;


__________________________________________________________________
-- table join by JSON (sub)attributes
--
-- get the name of its region for each country - sol. 1
SELECT  cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,
	nation ->> 'n_name' AS n_name,
	region ->> 'r_name' AS r_name
FROM sf_0_01__schema_250__flat_JSON.nation
	INNER JOIN sf_0_01__schema_250__flat_JSON.region
		ON (nation ->> 'n_regionkey') = (region ->> 'r_regionkey') ;

--
-- get the name of its region for each country - sol. 2
SELECT  cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,
	nation ->> 'n_name' AS n_name,
	region ->> 'r_name' AS r_name
FROM sf_0_01__schema_250__flat_JSON.nation
	INNER JOIN sf_0_01__schema_250__flat_JSON.region
		ON cast(nation ->> 'n_regionkey' as integer) =
			cast (region ->> 'r_regionkey' as integer) ;
