----------------------------------------------------------
-- simple queries for a flat JSON TPC-H schema (sf 0.01)
----------------------------------------------------------
-- see the previous script (`A1...`) for table creation and
-- population


----------------------------------------------------------
-- Extract all content of a table containing only a JSON attribute
--
SELECT *
FROM sf_0_01__schema_250__flat_JSON.nation ;

SELECT *
FROM sf_0_01__schema_250__flat_JSON.region ;

SELECT *
FROM sf_0_01__schema_250__flat_JSON.part ;

SELECT *
FROM sf_0_01__schema_250__flat_JSON.partsupp ;

SELECT *
FROM sf_0_01__schema_250__flat_JSON.lineitem ;

SELECT *
FROM sf_0_01__schema_250__flat_JSON.lineitem_orders_customer ;

SELECT *
FROM sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation ;



----------------------------------------------------------
-- Check the table structure in the original (normalized) TPC-H schema
select * from nation

-- ...


----------------------------------------------------------
-- Extract JSON (sub)attributes as text
SELECT *,
	nation ->> 'n_nationkey' AS n_nationkey,  -- the data type is text here!
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation ;

----------------------------------------------------------
-- ...Cast is needed for preserving the original data type
SELECT *,
	cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,  -- the data type is ok now
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation ;


----------------------------------------------------------
-- Get, in a tabular format, the nations whose names starts with `R`
SELECT  cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation
WHERE (nation ->> 'n_name') LIKE 'R%' ;



----------------------------------------------------------
-- Display each supplier's nation

-- sol. 1 - DISTINCT
SELECT DISTINCT
	partsupp_supplier_nation ->> 's_name' AS s_name,
	partsupp_supplier_nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation
ORDER BY 1


-- sol. 2 - GROUP BY
SELECT
	partsupp_supplier_nation ->> 's_name' AS s_name,
	partsupp_supplier_nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation
GROUP BY 1, 2
ORDER BY 1





----------------------------------------------------------
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


			SELECT count(*) FROM sf_0_01__schema_250__flat_JSON.lineitem_orders_customer

			SELECT count(*) FROM sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation

			------------------------------------------------------
			-- 		get the geografical region of each supplier
			------------------------------------------------------


			-- 1. in the original fully normalized TPCH schema the solution was:
			select * from region
			select * from nation
			select * from supplier

			select s_name, n_name, r_name
			from supplier
				inner join nation on supplier.s_nationkey = nation.n_nationkey
				inner join region on nation.n_regionkey = region.r_regionkey
			order by 1



			-- 2 in our schema, the tables are different, so:
			select * from sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation ;
			select * from sf_0_01__schema_250__flat_JSON.region ;



			select distinct
				partsupp_supplier_nation ->> 's_name' as supplier_name,
				partsupp_supplier_nation ->> 'n_name' as country_name,
				region ->> 'r_name' as region_name
			from sf_0_01__schema_250__flat_JSON.partsupp_supplier_nation
				inner join sf_0_01__schema_250__flat_JSON.region
				on partsupp_supplier_nation ->> 'n_regionkey' = region ->> 'r_regionkey'
			order by 1



			------------------------------------------------------
			-- 		get the geografical region of each customer
			------------------------------------------------------
