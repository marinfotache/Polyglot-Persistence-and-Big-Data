----------------------------------------------------------------------------------
--         Create, populate and query a table (`orders2`) containing JSON data
--                      nested on a two levels
----------------------------------------------------------------------------------


----------------------------------------------------------------------------------
--                      create table (no predefined schema)
----------------------------------------------------------------------------------

DROP TABLE IF EXISTS orders2 ;

CREATE TABLE orders2 (
 ID serial NOT NULL PRIMARY KEY,
 info jsonb NOT NULL
);


----------------------------------------------------------------------------------
--                     populate the table with three records
-- attribute `orders` is an array of documents; each element in `orders`
-- contain an array (the second level) of items
----------------------------------------------------------------------------------


INSERT INTO orders2 (info)
VALUES ( '
	{
	"customer_name": "Lily Bush",
	"customer_address": "Adress of Lily Bush",
	"orders" : [
		{"orderid": 11,
			"orderdate": "2018-04-01",
			"items": [
		 		{"product": "Diaper","qty": 111},
	 			{"product": "Toy Car","qty": 112},
	 			{"product": "Toy Train","qty": 113}]},
		{"orderid": 12,
			"orderdate": "2018-04-01",
			"items": [
	 			{"product": "Toy Car","qty": 121},
	 			{"product": "Toy Train","qty": 122}]},
		{"orderid": 13,
		"orderdate": "2018-04-02",
			"items": [
		 		{"product": "Diaper","qty": 131},
	 			{"product": "Toy Train","qty": 132}]},
    	{"orderid": 14,
  			"orderdate": "2018-04-03",
  			"items": [
  		 		{"product": "Diaper","qty": 150}]}
			]
	}') ;


INSERT INTO orders2 (info)
VALUES ( '
	{
	"customer_name": "Josh William",
	"customer_address": "Adress of Josh William",
	"orders" : [
		{"orderid": 21,
		"orderdate": "2018-04-01",
		"items": [
		 	{"product": "Diaper","qty": 211},
	 		{"product": "Toy Car","qty": 212},
	 		{"product": "Toy Train","qty": 213}]},
		{"orderid": 22,
		"orderdate": "2018-04-02",
		"items": [
	 		{"product": "Toy Car","qty": 221},
	 		{"product": "Toy Train","qty": 222}]},
		{"orderid": 23,
		"orderdate": "2018-04-02",
		"items": [
		 	{"product": "Diaper","qty": 231},
	 		{"product": "Toy Car","qty": 232},
	 		{"product": "Toy Train","qty": 233}]}
				]
	}') ;



INSERT INTO orders2 (info)
VALUES ( '
	{
	"customer_name": "Mary Clark",
	"customer_address": "Adress of Mary Clark",
	"orders" : [
		{"orderid": 31,
		"orderdate": "2018-04-01",
		"items": [
	 		{"product": "Toy Car","qty": 311},
	 		{"product": "Toy Train","qty": 312}]},
		{"orderid": 32,
		"orderdate": "2018-04-02",
		"items": [
	 		{"product": "Toy Car","qty": 321},
		 	{"product": "Diaper","qty": 322},
	 		{"product": "Toy Train","qty": 323}]},
		{"orderid": 33,
		"orderdate": "2018-04-02",
		"items": [
		 	{"product": "Diaper","qty": 331},
	 		{"product": "Toy Car","qty": 332},
	 		{"product": "Toy Train","qty": 333}]}
				]
	}') ;


-------------------------------------------------------------------------------------
--        access simple/scalar (sub)attributes/fields of a JSON attribute
-------------------------------------------------------------------------------------

-- display the entire content of table `orders`
select * from orders2 ;


--
-- extract two scalar sub-attributes of the `info` attribute, along
-- with an json array and get the array length
SELECT
  cast (info ->> 'customer_name' as character varying) AS customer_name,
  cast (info ->> 'customer_address' as character varying) AS customer_address,
  info -> 'orders' AS orders__json_array ,
  json_array_length((info -> 'orders')::JSON) n_of_customer_orders
FROM orders2;


--
-- extract the customer(s) with the largest number of orders
WITH
temp AS (
	SELECT
  	cast (info ->> 'customer_name' as character varying) AS customer_name,
  	json_array_length((info -> 'orders')::JSON) n_of_customer_orders
	FROM orders2
		)
SELECT *
FROM temp
WHERE n_of_customer_orders IN (
	SELECT MAX(n_of_customer_orders) FROM temp
	);



-------------------------------------------------------------------------------------
-- 						             one-level nesting
-------------------------------------------------------------------------------------

-- unnest/unwind the first-level array with `jsonb_array_elements` in SELECT
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	jsonb_array_elements(info -> 'orders') as customer_order
FROM orders2 ;

-- unnest/unwind the first-level array with `jsonb_array_elements` in LATERAL JOIN
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	customer_order, record_number
FROM orders2
   LEFT JOIN LATERAL jsonb_array_elements(info -> 'orders')
   		WITH ORDINALITY AS x (customer_order, record_number) ON true




--------------------------------------------------------------------------
--        display values for attributes of the nested array

-- with `jsonb_array_elements` included in SELECT
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid,
	cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items,
    json_array_length((info -> 'orders')::JSON) n_of_customer_orders
FROM orders2;

-- with LATERAL JOIN
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	cast (customer_order ->> 'orderid' as integer) as orderid,
	cast (customer_order ->> 'orderdate' as date) as orderdate,
	customer_order -> 'items' as order_items,
    json_array_length((info -> 'orders')::JSON) n_of_customer_orders
FROM orders2
   LEFT JOIN LATERAL jsonb_array_elements(info -> 'orders')
   		WITH ORDINALITY AS x (customer_order, record_number) ON true



--------------------------------------------------------------------------
--          Get the number of items in each order


-- ... with `jsonb_array_elements` included in SELECT
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid,
	cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items,
  	json_array_length((jsonb_array_elements(info -> 'orders')->> 'items')::JSON) n_of_order_items
FROM orders2;


-- ... with LATERAL JOIN
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	cast (customer_order ->> 'orderid' as integer) as orderid,
	cast (customer_order ->> 'orderdate' as date) as orderdate,
	customer_order -> 'items' as order_items,
    json_array_length((customer_order -> 'items')::JSON) n_of_order_items
FROM orders2
   LEFT JOIN LATERAL jsonb_array_elements(info -> 'orders')
   		WITH ORDINALITY AS x (customer_order, record_number) ON true



------------------------------------------------------------------------------------
--  Extract the order(s) (and its/their customer) with the largest number of items

-- ... with CTE
WITH
flattened AS (
	SELECT
		info ->> 'customer_name' AS customer_name,
		cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) AS orderid,
		cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) AS orderdate,
  		json_array_length((jsonb_array_elements(info -> 'orders')->> 'items')::JSON) AS n_of_order_items
	FROM orders2)
SELECT *
FROM flattened
WHERE n_of_order_items IN (
	SELECT MAX(n_of_order_items)
	FROM flattened
	);


-- with LATERAL JOIN
WITH flattened AS (
	SELECT
		info ->> 'customer_name' AS customer_name,
		cast (customer_order ->> 'orderid' as integer) as orderid,
		cast (customer_order ->> 'orderdate' as date) as orderdate,
	    json_array_length((customer_order -> 'items')::JSON) n_of_order_items
	FROM orders2
   	LEFT JOIN LATERAL jsonb_array_elements(info -> 'orders')
   		WITH ORDINALITY AS x (customer_order, record_number) ON true
	)
SELECT *
FROM flattened
WHERE n_of_order_items IN (
	SELECT MAX(n_of_order_items)
	FROM flattened
	);




-------------------------------------------------------------------------------------
--                        two-level nesting
-------------------------------------------------------------------------------------

-------------------------------------------------------------------------------------
--            unnest arrays on both levels in a sigle step

-- ... with `jsonb_array_elements` included in SELECT
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid, -- first-level unnesting
	cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate, -- first-level unnesting
	jsonb_array_elements(info -> 'orders')-> 'items' as order_items,
	jsonb_array_elements(info -> 'orders' -> 'items')->> 'product' as product__wrong, -- NOT WORKING !!!
	jsonb_array_elements(jsonb_array_elements(info -> 'orders')-> 'items')->> 'product' AS product, -- second-level unnesting
	cast (jsonb_array_elements(jsonb_array_elements(info -> 'orders')-> 'items')->> 'qty' as numeric) AS quantity  -- second-level unnesting
FROM orders2;


-- ... with LATERAL JOIN
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	cast (customer_order ->> 'orderid' as integer) AS orderid,
	cast (customer_order ->> 'orderdate' as date) AS orderdate,
	customer_order -> 'items' AS order_items,
	items ->> 'product'  AS product,   -- second-level unnesting
	cast (items ->> 'qty' as numeric) AS quantity  -- second-level unnesting

FROM orders2
   LEFT JOIN LATERAL jsonb_array_elements(info -> 'orders')
   		WITH ORDINALITY AS x (customer_order, record_number) ON true
   LEFT JOIN LATERAL jsonb_array_elements(x.customer_order -> 'items')
   		WITH ORDINALITY AS y (items, record_number) ON true



-------------------------------------------------------------------------------------
--         unnest arrays on both levels in two steps with `jsonb_array_elements`
--
WITH
flattened1 AS (
	SELECT
		info ->> 'customer_name' AS customer_name,
		info ->> 'customer_address' AS customer_address,
		cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid,
		cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate,
		cast (jsonb_array_elements(info -> 'orders')-> 'items' as jsonb) as order_items
	FROM orders2),
flattened2 AS (
	SELECT customer_name, customer_address,
		orderid, orderdate,
		jsonb_array_elements(order_items) as order_items,
		cast (jsonb_array_elements(order_items) ->> 'product' as character varying) AS product_name,
		cast (jsonb_array_elements(order_items) ->> 'qty' as numeric) AS quantity
	FROM flattened1)
SELECT *
FROM flattened2 ;






-------------------------------------------------------------------------------------
--              Display the total units sold on each product


-- solution with `jsonb_array_elements` and table expressions (CTE)
WITH
flattened1 AS (
	SELECT
		info ->> 'customer_name' AS customer_name,
		info ->> 'customer_address' AS customer_address,
		cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid,
		cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate,
		cast (jsonb_array_elements(info -> 'orders')-> 'items' as jsonb) as order_items
	FROM orders2),
flattened2 AS (
	SELECT customer_name, customer_address,
		orderid, orderdate,
		jsonb_array_elements(order_items) as order_items,
		cast (jsonb_array_elements(order_items) ->> 'product' as character varying) AS product_name,
		cast (jsonb_array_elements(order_items) ->> 'qty' as numeric) AS quantity
	FROM flattened1),
units_sold AS (
	SELECT product_name, SUM (quantity) AS prod_units_sold
	FROM flattened2
	GROUP BY product_name
	)
SELECT *
FROM units_sold


-- solution with LATERAL JOIN

-- solution with LATERAL JOIN
SELECT
		items ->> 'product'  AS product_name,
		SUM (cast (items ->> 'qty' as numeric)) AS prod_units_sold
FROM orders2
   LEFT JOIN LATERAL jsonb_array_elements(info -> 'orders')
   		WITH ORDINALITY AS x (customer_order, record_number) ON true
   LEFT JOIN LATERAL jsonb_array_elements(x.customer_order -> 'items')
   		WITH ORDINALITY AS y (items, record_number) ON true
GROUP BY items ->> 'product'
		
