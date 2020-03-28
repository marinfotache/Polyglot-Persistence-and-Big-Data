----------------------------------------------------------
-- simple queries for a flat JSON TPC-H schema (sf 0.01)
----------------------------------------------------------
-- see the previous script (`A1...`) for table creation and
-- population



----------------------------------------------------------
-- extract all content of a table containing only a JSON attribute
SELECT *
FROM sf_0_01__schema_250__flat_JSON.nation ;


----------------------------------------------------------
-- check the table structure in the original (normalized) TPC-H schema
select * from nation


----------------------------------------------------------
-- extract JSON (sub)attributes as text
SELECT *,
	nation ->> 'n_nationkey' AS n_nationkey,  -- the data type is text here!
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation ;

----------------------------------------------------------
-- cast needed for preserving the data type
SELECT *,
	cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,  -- the data type is ok now
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation ;


----------------------------------------------------------
-- get, as a tabular result, the nations whose names starts with `R`
SELECT  cast (nation ->> 'n_nationkey' as integer) AS n_nationkey,
	nation ->> 'n_name' AS n_name
FROM sf_0_01__schema_250__flat_JSON.nation
WHERE (nation ->> 'n_name') LIKE 'R%' ;


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
