--=========================================================================================
-- 										JSON Data Management in PostgreSQL
--=========================================================================================
-- 									D5 SQL queries for nested JSON data
--=========================================================================================


--=========================================================================================
--                      			a JSON `nested` scenario:
-- `counties__JSON_NESTED` is based on `counties` table in the normalized schema
-- `postcodes__JSON_NESTED` is based on `postcodes` table in the normalized schema
-- `customers__JSON_NESTED` is based on `customers` + `contacts` + `people`  tables in the normalized schema
-- `products__JSON_NESTED` is based on `products` table in the normalized schema
-- `invoices__JSON_NESTED` is based on `invoices` + `invoice_details` + `products`  tables in the normalized schema
-- `receipts__JSON_NESTED` is based on `receipts` + `receipt_details` tables in the normalized schema
--=========================================================================================


-------------------------------------------------------------------------
-- display invoice number and date as separate attributes, and also
--  display in the JSON format all the lines of each invoice
SELECT
	CAST (json_data ->> 'invoiceno' AS NUMERIC) AS invoiceno,
	CAST (json_data ->> 'invoicedate' AS DATE) AS invoicedate,
	json_data -> 'invoice_details' AS invoice_details
FROM invoices__JSON_NESTED


-------------------------------------------------------------------------
-- display invoice number and date as separate attributes, and also
--  display in the JSON format all the lines of each invoice;
--  also extract on a separate column (as JSONB) only the first line of each invoice
SELECT
	CAST (json_data ->> 'invoiceno' AS NUMERIC) AS invoiceno,
	CAST (json_data ->> 'invoicedate' AS DATE) AS invoicedate,
	json_data -> 'invoice_details' AS invoice_details,
	json_data -> 'invoice_details' -> 0 AS invoice_first_line
FROM invoices__JSON_NESTED



-------------------------------------------------------------------------
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



-------------------------------------------------------------------------
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


-------------------------------------------------------------------------
-- For each receipt, display as JSON all payment details
--
SELECT
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details
FROM receipts__JSON_NESTED



-------------------------------------------------------------------------
-- Display (as JSON) each paid invoice, along with payment id and date
--

-- solution with `jsonb_array_elements`
SELECT
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details_json,
	jsonb_array_elements(json_data -> 'receipt_details')  as receipt_details_tabular
FROM receipts__JSON_NESTED

-- solution with LATERAL JOIN
SELECT
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details_json,
	receipt_details_tabular
FROM receipts__JSON_NESTED
   LEFT JOIN LATERAL jsonb_array_elements(json_data -> 'receipt_details')
   		WITH ORDINALITY AS x (receipt_details_tabular, record_number) ON true



-------------------------------------------------------------------------
-- Display, in tabular form, each paid invoice, along with its
-- payment id and date

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
-- a solution based on `jsonb_each` and `jsonb_array_elements`
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
-- another solution based on `jsonb_array_elements`, `jsonb_array_elements`
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


-- solution with `LATERAL JOIN`
SELECT
	CAST (json_data ->> 'receiptid' AS NUMERIC) AS receiptid,
	CAST (json_data ->> 'receiptdate' AS DATE) AS receiptdate,
	json_data -> 'receipt_details' AS receipt_details_json,
	CAST (paid_invoices ->> 'invoiceno' AS NUMERIC) as invoiceno,
	CAST (paid_invoices ->> 'amount' AS NUMERIC) as amount
FROM receipts__JSON_NESTED
   LEFT JOIN LATERAL jsonb_array_elements(json_data -> 'receipt_details')
   		WITH ORDINALITY AS x (paid_invoices, record_number) ON true


-------------------------------------------------------------------------
-- 						Display sales on each product

-- solution with `LATERAL JOIN`
with temp as (
	select
		invoice_line -> 'product' ->> 'productname' as product_name,
		cast (invoice_line ->> 'quantity' as numeric) as quantity,
		cast (invoice_line ->> 'unitprice' as numeric) as unit_price,
		cast (invoice_line -> 'product' ->> 'vatpercent'  as numeric) as vat_percent
	from invoices__JSON_NESTED
   	LEFT JOIN LATERAL jsonb_array_elements(json_data -> 'invoice_details')
   		WITH ORDINALITY AS x (invoice_line, record_number) ON true
		)
select product_name, sum(quantity * unit_price * (1 + vat_percent)) as prod_sales
from temp
group by product_name
order by 1


-------------------------------------------------------------------------
-- 						Display top 3 counties for the sales amount
