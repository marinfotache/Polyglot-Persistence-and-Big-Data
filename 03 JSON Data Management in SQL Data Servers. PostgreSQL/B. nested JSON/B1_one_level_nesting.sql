----------------------------------------------------------------------------------
--         Create, populate and query a table (`orders`) containing JSON data
--                      nested on a single level
----------------------------------------------------------------------------------


----------------------------------------------------------------------------------
--                      create table (no predefined schema)
----------------------------------------------------------------------------------

DROP TABLE orders ;

CREATE TABLE orders (
 ID serial NOT NULL PRIMARY KEY,
 info jsonb NOT NULL
);



----------------------------------------------------------------------------------
--                     populate the table with three records
-- attribute `items` is an array of documents
----------------------------------------------------------------------------------

INSERT INTO orders (info)
VALUES ( '{
	"orderid": 1,
	"orderdate": "2018-04-01",
	"customer": "Lily Bush",
	"items": [
	 	{"product": "Diaper","qty": 21},
	 	{"product": "Toy Car","qty": 31},
	 	{"product": "Toy Train","qty": 2}]
	}') ;

INSERT INTO orders (info)
VALUES ( '{
	"orderid": 2,
	"orderdate": "2018-04-01",
	"customer": "Josh William",
	"items": [
	 	{"product": "Diaper","qty": 22},
	 	{"product": "Toy Car","qty": 32}]
	}') ;

INSERT INTO orders (info)
VALUES ( '{
	"orderid": 3,
	"orderdate": "2018-04-02",
	"customer": "Mary Clark",
	"items": [
	 	{"product": "Toy Train","qty": 33},
	 	{"product": "Diaper","qty": 23}
		]
	}') ;



-------------------------------------------------------------------------------------
--        access simple/scalar (sub)attributes/fields of a JSON attribute
-------------------------------------------------------------------------------------

-- display the entire content of table `orders`
select * from orders ;

-- extract, as scalars, some sub-attributes of the `info` attribute ( which
-- is of type JSON) ): `orderid`, `orderdate`, `customer`
-- in the result,  all columns results are of type text
SELECT info ->> 'orderid' AS orderid,   -- notice the datatype in the result
	info ->> 'orderdate' AS orderdate,   -- notice the datatype in the result
	info ->> 'customer' AS customer
FROM orders;

-- the same results, but this time the columns are casted appropriately
SELECT cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	cast (info ->> 'customer' as character varying) AS customer
FROM orders;


------------------------------------------------------------------
--      display all the orders of customer 'Josh William'
SELECT info ->> 'orderid' AS orderid,
	info ->> 'orderdate' AS orderdate,
	info ->> 'customer' AS customer,
	info ->> 'items' AS items
FROM orders
WHERE info ->> 'customer' = 'Josh William';



------------------------------------------------------------------
-- display all the orders issued on April 01, 2018
SELECT cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	cast (info ->> 'customer' as character varying) AS customer,
	info ->> 'items' AS items
FROM orders
WHERE cast (info ->> 'orderdate' as date) = DATE'2018-04-01';




-------------------------------------------------------------------------------------
--  access individual elements of array (sub)attributes/fields of a JSON attribute
-------------------------------------------------------------------------------------


-- attribute `items` is of type array:

-- ...here it will be displayed as JSONB
SELECT info -> 'items' AS items
FROM orders;

-- ...and here it will be displayed as text
SELECT info ->> 'items' AS items
FROM orders;


------------------------------------------------------------------
-- display only the first element in array `items`
-- for each table row
SELECT cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	cast (info ->> 'customer' as character varying) AS customer,
	info ->> 'items' AS all_items,
	info -> 'items' ->> 0  as first_item
FROM orders;


-------------------------------------------------------------------------
-- extract (the scalar) attribute `product` of the first element in array
--  `items` for each table row (along with other table attributes (for a
-- better display))
SELECT info ->> 'orderid' AS orderid,
	info ->> 'orderdate' AS orderdate,
	info ->> 'customer' AS customer,
	info ->> 'items' AS all_items,
	info -> 'items' ->> 0  as first_item,
	info -> 'items' -> 0 ->> 'product'  as product_name_of_the_first_item
FROM orders;




-------------------------------------------------------------------------------------
--  access all elements of an array (sub)attribute/field of a JSON attribute as
--                  a recordset with function `jsonb_array_elements`
-------------------------------------------------------------------------------------


-- first example of function `jsonb_array_elements`
SELECT *
FROM orders,
  jsonb_array_elements(info -> 'items')


--
-- Extract all the product names (this is the property `product` of elements of array
--  `items` in the JSON attribute `info`)
SELECT jsonb_array_elements(info -> 'items')->> 'product' as product_names
FROM orders

-- ... the same result, but remove duplicates and order result by product name
SELECT DISTINCT jsonb_array_elements(info -> 'items')->> 'product' as product_names
FROM orders
ORDER BY 1


--
-- function `jsonb_array_elements` operates like an `explode` operator
-- it is similar to `unnest` (tidyverse) or `unwind` (MongoDB Aggregation Framework)
SELECT cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	info ->> 'items' AS all_items,
	jsonb_array_elements(info -> 'items')->> 'product' as product_name
FROM orders;


------------------------------------------------------------------
-- display as a recordset all order lines; for each line, extract product name (`product`)
-- and sold quantity

SELECT id, info,
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	info ->> 'items' AS all_items,
	jsonb_array_elements(info -> 'items')->> 'product' as product_name,
	jsonb_array_elements(info -> 'items')->> 'qty' as quantity
FROM orders;


------------------------------------------------------------------
-- extract all products bought by customer 'Lily Bush'

-- ... the simplest solution
SELECT DISTINCT
	jsonb_array_elements(info -> 'items')->> 'product' as product_name
FROM orders
WHERE info ->> 'customer' = 'Lily Bush'
ORDER BY 1

-- ... solution based on a Common Table Expression
WITH flattened AS (
	SELECT orders.*,
		jsonb_array_elements(info -> 'items')->> 'product' as product_name
	FROM orders
	WHERE info ->> 'customer' = 'Lily Bush')
SELECT DISTINCT product_name
FROM flattened
ORDER BY 1


-------------------------------------------------------------------------------------------
-- Extract all customers and orders when more than 31 units of product `Toy Car` were sold

-- ... next solution DOES NOT WORK! (error message: `set-returning functions are not allowed in WHERE`)
SELECT
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	jsonb_array_elements(info -> 'items')->> 'product' as product_name,
	cast (jsonb_array_elements(info -> 'items')->> 'qty' as numeric) as quantity
FROM orders
WHERE jsonb_array_elements(info -> 'items')->> 'product' = 'Toy Car' AND
	cast (jsonb_array_elements(info -> 'items')->> 'qty' as numeric) > 31

-- ... solution based on a Common Table Expression
WITH flattened AS (
	SELECT
		cast (info ->> 'orderid' as integer) AS orderid,
		cast (info ->> 'orderdate' as date) AS orderdate,
		info ->> 'customer' AS customer,
		jsonb_array_elements(info -> 'items')->> 'product' as product_name,
		cast (jsonb_array_elements(info -> 'items')->> 'qty' as numeric) as quantity
	FROM orders)
SELECT DISTINCT *
FROM flattened
WHERE product_name = 'Toy Car' AND quantity > 31


----------------------------------------------------------------------------
-- display the product(s) with the largest number of units sold
-- ... solution based on a Common Table Expression and `jsonb_array_elements`
WITH
flattened AS (
	SELECT
		cast (info ->> 'orderid' as integer) AS orderid,
		cast (info ->> 'orderdate' as date) AS orderdate,
		info ->> 'customer' AS customer,
		jsonb_array_elements(info -> 'items')->> 'product' as product_name,
		cast (jsonb_array_elements(info -> 'items')->> 'qty' as numeric) as quantity
	FROM orders),
products_grouped AS (
	SELECT product_name, SUM(quantity) AS n_of_units_sold
	FROM flattened
	GROUP BY product_name
	)
SELECT DISTINCT *
FROM products_grouped
WHERE n_of_units_sold = (SELECT MAX(n_of_units_sold) FROM products_grouped)



-------------------------------------------------------------------------------------
--  access all elements of an array (sub)attribute/field of a JSON attribute as
--                  a recordset with function `jsonb_to_recordset`
-------------------------------------------------------------------------------------

--
-- ... the same result of the next query which uses
--     function `jsonb_array_elements` included in SELECT
SELECT
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	cast(jsonb_array_elements(info -> 'items')->> 'product' as varchar) as product,
	cast (jsonb_array_elements(info -> 'items')->> 'qty' as numeric) as qty
FROM orders;

-- ... can be achieved with function `jsonb_to_recordset` (included in FROM)
SELECT
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	product, qty
FROM orders,
  jsonb_to_recordset(info -> 'items') as (product varchar, qty numeric )



------------------------------------------------------------------
-- Extract all products bought by customer 'Lily Bush'

--
-- solution with `jsonb_to_recordset`

SELECT DISTINCT product AS product_name
FROM
  	(SELECT *
  	 FROM orders
  	 WHERE info ->> 'customer' = 'Lily Bush') lily,
    	jsonb_to_recordset(info -> 'items') as (product varchar, qty numeric )
ORDER BY 1


-------------------------------------------------------------------------------------------
-- Extract all customers and orders when more than 31 units of product `Toy Car` were sold

-- ... solution based on `jsonb_to_recordset` and CTE

WITH flattened AS (
	SELECT orderid, orderdate, customer, product, qty
	FROM
		(SELECT
			cast (info ->> 'orderid' as integer) AS orderid,
			cast (info ->> 'orderdate' as date) AS orderdate,
			info ->> 'customer' AS customer,
	 		info -> 'items' AS items
	 	FROM orders) x,
  		jsonb_to_recordset(items) as (product varchar, qty numeric )
				)
SELECT *
FROM flattened
WHERE product = 'Toy Car' AND qty > 31

ORDER BY 1


----------------------------------------------------------------------------
-- display the product(s) with the largest number of units sold
-- ... solution based on  `jsonb_to_recordset`

-- to do!!!




------------------------------------------------------------------
--      optiunile LATERAL (JOIN) ... WITH ORDINALITY
------------------------------------------------------------------

SELECT id, info,
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	*
FROM orders
   , lateral jsonb_array_elements(info -> 'items') WITH ORDINALITY AS x (val, record_number)


-- optiunile LATERAL (JOIN) ... WITH ORDINALITY si `jsonb_each_text`
-- adapted from a solution presented on:
-- https://stackoverflow.com/questions/51045754/unnesting-a-list-of-json-objects-in-postgresql

SELECT id, info,
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	*
FROM orders
   	  , lateral jsonb_array_elements(info -> 'items') WITH ORDINALITY AS x (val, record_number)
	   , lateral jsonb_each_text(x.val) y


--
-- `LEFT JOIN LATERAL...` notation
SELECT id, info,
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	*
FROM orders
   LEFT JOIN LATERAL jsonb_array_elements(info -> 'items')
   		WITH ORDINALITY AS x (val, record_number) ON true



-------------------------------------------------------------------------------------------
-- Extract all customers and orders when more than 31 units of product `Toy Car` were sold
--
-- solution with `lateral.... with ordinality...`

SELECT
	cast (info ->> 'orderid' as integer) AS orderid,
	cast (info ->> 'orderdate' as date) AS orderdate,
	info ->> 'customer' AS customer,
	*
FROM orders
   	, lateral jsonb_array_elements(info -> 'items') WITH ORDINALITY AS x (val, record_number)
WHERE val ->> 'product' = 'Toy Car' AND
	 cast (val ->> 'qty' as numeric) > 31



-- solution using `LATERAL (JOIN) ... WITH ORDINALITY` si `jsonb_each_text`
WITH temp AS (
   	SELECT
   		cast (info ->> 'orderid' as integer) AS orderid,
   		cast (info ->> 'orderdate' as date) AS orderdate,
   		info ->> 'customer' AS customer,
   		*
   	FROM orders
      		, lateral jsonb_array_elements(info -> 'items') WITH ORDINALITY AS x (val, record_number)
   		, lateral jsonb_each_text(x.val) y
   		)
SELECT *
FROM temp
WHERE orderid || '-'|| record_number IN (
   	SELECT orderid || '-' ||record_number
       FROM temp
   	WHERE key = 'product' AND value = 'Toy Car'
   	INTERSECT
   	SELECT orderid || '-' || record_number
       FROM temp
   	WHERE key = 'qty' AND cast (value as numeric) > 31
   	)




-------------------------------------------------------------------------------------
--                  combine `jsonb_array_elements` with `jsonb_each`
-------------------------------------------------------------------------------------

-- adapted from a solution presented on:
-- https://stackoverflow.com/questions/45807712/postgresql-aggregate-json-recordset-keys-by-row
SELECT *
FROM (
    SELECT id, info ->> 'customer' as customer,
        (jsonb_each(jsonb_array_elements(info ->'items'))).key as k,
       (jsonb_each(jsonb_array_elements(info ->'items'))).value::text as v
 			FROM orders
	) AS json_data



-------------------------------------------------------------------------------------
--                    Exercises (to do as lab activities):
-------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------
--                    Display the number of items bought by each customer
-------------------------------------------------------------------------------------
--               Display the number of items bought by each customer
WITH temp AS (
	SELECT DISTINCT
		info ->> 'customer' as customer_name,
		item ->> 'product' as product_name
	FROM orders
   	LEFT JOIN LATERAL jsonb_array_elements(info -> 'items')
   			WITH ORDINALITY AS x (item, record_number) ON true
	)
SELECT customer_name, COUNT(*) AS n_of_products
FROM temp
GROUP BY customer_name


-------------------------------------------------------------------------------------
--    Display the common items bought by both customers `Lily Bush` and `Mary Clark`



-------------------------------------------------------------------------------------
--         Display the customer who bought the largest number of product units
