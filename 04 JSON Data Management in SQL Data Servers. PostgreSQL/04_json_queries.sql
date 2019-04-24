--=========================================================================================
-- 						JSON Data Management in PostgreSQL
--=========================================================================================
-- 							04 SQL queries for JSON data 
--=========================================================================================


--=========================================================================================
--                      					JSON `flat` scenario:
-- for each normalized table, a table will be created with just two attributes, an 
-- `id` (autoincremented) and `json_data` with all attributes taken from the 
-- relational tables; no foreign keys will be declared, but only primary key and some 
-- alternate keys and not null restrictions
--=========================================================================================


--
-- Extract (as JSON) counties in `Moldova` region
SELECT *
FROM counties__JSON_FLAT x
WHERE json_data ->> 'region' = 'Moldova'

--
-- Extract (as JSON) county names in `Moldova` region
SELECT json_data -> 'countyname' as countyname_json
FROM counties__JSON_FLAT x
WHERE json_data ->> 'region' = 'Moldova'

--
-- Extract (as text) county names in `Moldova` region
SELECT json_data ->> 'countyname' as countyname_text
FROM counties__JSON_FLAT x
WHERE json_data ->> 'region' = 'Moldova'



--
-- Display, for each customer, the place and the county of its address
-- (as JSONB)
SELECT *
FROM customers__JSON_FLAT cust
	INNER JOIN postcodes__JSON_FLAT p
		ON cust.json_data ->> 'postcode' = p.json_data ->> 'postcode'
	INNER JOIN counties__JSON_FLAT counties
		ON p.json_data ->> 'countycode' = counties.json_data ->> 'countycode'


--
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


--
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





--=========================================================================================
--                      			a JSON `nested` scenario:
-- we'll create only one of many possible scenarios with the following tables:
-- `counties__JSON_NESTED` based on `counties` table in the normalized schema
-- `postcodes__JSON_NESTED` based on `postcodes` table in the normalized schema
-- `customers__JSON_NESTED` based on `customers` + `contacts` + `people`  tables in the normalized schema
-- `products__JSON_NESTED` based on `products` table in the normalized schema
-- `invoices__JSON_NESTED` based on `invoices` + `invoice_details` + `products`  tables in the normalized schema
-- `receipts__JSON_NESTED` based on `receipts` + `receipt_details` tables in the normalized schema
--=========================================================================================


-- 
-- display invoice number and date as separate attributes, and also
--  display in the JSON format all the lines of each invoice
SELECT 
	CAST (json_data ->> 'invoiceno' AS NUMERIC) AS invoiceno,
	CAST (json_data ->> 'invoicedate' AS DATE) AS invoicedate,
	json_data -> 'invoice_details' AS invoice_details
FROM invoices__JSON_NESTED


-- 
-- display invoice number and date as separate attributes, and also
--  display in the JSON format all the lines of each invoice;
--  also extract on a separate column (as JSONB) only the first line of each invoice
SELECT 
	CAST (json_data ->> 'invoiceno' AS NUMERIC) AS invoiceno,
	CAST (json_data ->> 'invoicedate' AS DATE) AS invoicedate,
	json_data -> 'invoice_details' AS invoice_details,
	json_data -> 'invoice_details' -> 0 AS invoice_first_line
FROM invoices__JSON_NESTED



-- 
-- display invoice number and date as separate attributes, and also
--  display in the JSON format all the lines of each invoice;
--  also extract (as JSON) on a separate column only the first line of each invoice,
--  and the product (as JSON) appearing in the first line 
--   of the invoice
SELECT 
	CAST (json_data ->> 'invoiceno' AS NUMERIC) AS invoiceno,
	CAST (json_data ->> 'invoicedate' AS DATE) AS invoicedate,
	json_data -> 'invoice_details' AS invoice_details,
	json_data -> 'invoice_details' -> 0 AS invoice_first_line,
	json_data -> 'invoice_details' -> 0 -> 'product' AS product_in_the_invoice_first_line
FROM invoices__JSON_NESTED



-- 
-- display invoice number and date as separate attributes, and also:
--  * display in the JSON format all the lines of each invoice;
--  * extract (as JSON) on a separate column only the first line of each invoice,
--  * extract the product (as JSON) appearing in the first line 
--  * extract (as text) only the name of the product appearing in the first line 
SELECT 
	CAST (json_data ->> 'invoiceno' AS NUMERIC) AS invoiceno,
	CAST (json_data ->> 'invoicedate' AS DATE) AS invoicedate,
	json_data -> 'invoice_details' AS invoice_details,
	json_data -> 'invoice_details' -> 0 AS invoice_first_line,
	json_data -> 'invoice_details' -> 0 -> 'product' AS product_in_the_invoice_first_line,
	json_data -> 'invoice_details' -> 0 -> 'product' ->> 'productname' 
		AS product_name_in_the_invoice_first_line
FROM invoices__JSON_NESTED


--
-- For each receipt, display as JSON all payment details
--
SELECT 
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details	
FROM receipts__JSON_NESTED


--
-- Display (as JSON) each paid invoice, along with payment id and date
--

-- solution with `jsonb_array_elements`
SELECT 
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details_json,
	jsonb_array_elements(json_data -> 'receipt_details')  as receipt_details_tabular		
FROM receipts__JSON_NESTED


--
-- Display in tabular for each paid invoice, along with its payment id and date
--

-- solution with `jsonb_array_elements`
SELECT 
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details_json,
	CAST (jsonb_array_elements(json_data -> 'receipt_details') ->> 'invoiceno' AS NUMERIC) as invoiceno,
	CAST (jsonb_array_elements(json_data -> 'receipt_details') ->> 'amount' AS NUMERIC) as amount
FROM receipts__JSON_NESTED



-- first (raw) solution with `jsonb_to_recordset`
SELECT 
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details_json, 
	*
FROM receipts__JSON_NESTED,
  jsonb_to_recordset(json_data -> 'receipt_details') as (
	  	invoiceno NUMERIC, 
	  	amount NUMERIC )


-- second (slighly refined) solution with `jsonb_to_recordset`
SELECT 
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	invoiceno, amount
FROM receipts__JSON_NESTED,
  jsonb_to_recordset(json_data -> 'receipt_details') as (
	  	invoiceno NUMERIC, 
	  	amount NUMERIC )



--
-- a solution base on on `jsonb_each` and `jsonb_array_elements`
-- with a different format of the result (the `long format` in `tidyverse`)
-- adapted from:
-- https://stackoverflow.com/questions/45807712/postgresql-aggregate-json-recordset-keys-by-row
SELECT *
FROM (
    SELECT 
		CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
		CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
        (jsonb_each(jsonb_array_elements(json_data ->'receipt_details'))).key as k,
        (jsonb_each(jsonb_array_elements(json_data ->'receipt_details'))).value::text as v
 	FROM receipts__JSON_NESTED
	) AS json_data


--
-- another solution base on `jsonb_array_elements`, `jsonb_array_elements` 
--  and `lateral`
-- with a different format of the result (the `long format` in `tidyverse`)
-- adapted from:
-- https://stackoverflow.com/questions/51045754/unnesting-a-list-of-json-objects-in-postgresql
SELECT 
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	y.key, y.value, x.record_number
FROM receipts__JSON_NESTED
   , lateral jsonb_array_elements(json_data ->'receipt_details') 
   		WITH ORDINALITY AS x (val, record_number)
   , lateral jsonb_each_text(x.val) y 






























