--=========================================================================================
-- 												JSON Data Management in PostgreSQL
--=========================================================================================
-- 										D4 SQL queries for flat JSON data
--=========================================================================================

--=========================================================================================
--                      					JSON `flat` scenario:
-- for each normalized table, a table was created with just two attributes, an
-- `id` (autoincremented) and `json_data` with all attributes taken from the
-- relational tables; no foreign keys declared, but only primary key and some
-- alternate keys and not null restrictions
--=========================================================================================


------------------------------------------------------------
-- Extract (as JSON) counties in `Moldova` region
SELECT *
FROM counties__JSON_FLAT x
WHERE json_data ->> 'region' = 'Moldova'


------------------------------------------------------------
-- Extract (as JSON) county names in `Moldova` region
SELECT json_data -> 'countyname' as countyname_json
FROM counties__JSON_FLAT x
WHERE json_data ->> 'region' = 'Moldova'


------------------------------------------------------------
-- Extract (as text) county names in `Moldova` region
SELECT json_data ->> 'countyname' as countyname_text
FROM counties__JSON_FLAT x
WHERE json_data ->> 'region' = 'Moldova'



------------------------------------------------------------
-- Display, for each customer, the place and the county of its address
-- (as JSONB)
SELECT *
FROM customers__JSON_FLAT cust
	INNER JOIN postcodes__JSON_FLAT p
		ON cust.json_data ->> 'postcode' = p.json_data ->> 'postcode'
	INNER JOIN counties__JSON_FLAT counties
		ON p.json_data ->> 'countycode' = counties.json_data ->> 'countycode'


------------------------------------------------------------
-- Display, for each customer, the place and the county of its address
-- (result will be in classic tabular (non-JSON) format)
-- Notice that each attribute will be of type TEXT, even the `customerid` !!!!
SELECT cust.json_data ->> 'customerid' AS customerid,
	cust.json_data ->> 'customername' AS customername,
	cust.json_data ->> 'fiscalcode' AS fiscalcode,
	cust.json_data ->> 'address' AS address,
	p.json_data ->> 'place' AS place,
	p.json_data ->> 'countycode' AS countycode,
	counties.json_data ->> 'countyname' AS countyname,
	counties.json_data ->> 'region' AS region,
	cust.json_data ->> 'phone' AS phone
FROM customers__JSON_FLAT cust
	INNER JOIN postcodes__JSON_FLAT p
		ON cust.json_data ->> 'postcode' = p.json_data ->> 'postcode'
	INNER JOIN counties__JSON_FLAT counties
		ON p.json_data ->> 'countycode' = counties.json_data ->> 'countycode'


------------------------------------------------------------
-- Display, for each customer, the place and the county of its address
-- (result will be in classic tabular (non-JSON) format)
-- Each attribute must be extracted in its original type
SELECT CAST (cust.json_data ->> 'customerid' AS NUMERIC) AS customerid,
	CAST (cust.json_data ->> 'customername' AS VARCHAR) AS customername,
	CAST (cust.json_data ->> 'fiscalcode' AS CHAR(9)) AS fiscalcode,   -- for `CHAR` one must specify the length
	CAST (cust.json_data ->> 'address' AS VARCHAR) AS address,
	CAST (p.json_data ->> 'place' AS VARCHAR) AS place,
	CAST (p.json_data ->> 'countycode' AS CHAR(2)) AS countycode,
	CAST (counties.json_data ->> 'countyname' AS VARCHAR) AS countyname,
	CAST (counties.json_data ->> 'region' AS VARCHAR) AS region,
	CAST (cust.json_data ->> 'phone' AS VARCHAR) AS phone
FROM customers__JSON_FLAT cust
	INNER JOIN postcodes__JSON_FLAT p
		ON cust.json_data ->> 'postcode' = p.json_data ->> 'postcode'
	INNER JOIN counties__JSON_FLAT counties
		ON p.json_data ->> 'countycode' = counties.json_data ->> 'countycode'
