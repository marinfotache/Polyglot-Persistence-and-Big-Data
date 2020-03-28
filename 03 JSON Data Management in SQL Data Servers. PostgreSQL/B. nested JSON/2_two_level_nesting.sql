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
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid,
	cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items,
  json_array_length((info -> 'orders')::JSON) n_of_customer_orders
FROM orders2;


---
-- get the number of items in each order
SELECT
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	cast (jsonb_array_elements(info -> 'orders')->> 'orderid' as integer) as orderid,
	cast(jsonb_array_elements(info -> 'orders')->> 'orderdate' as date) as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items,
  	json_array_length((jsonb_array_elements(info -> 'orders')->> 'items')::JSON) n_of_order_items
FROM orders2;


---
-- extract the order(s) (and its/their customer) with the largest number of items
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



-------------------------------------------------------------------------------------
--                        two-level nesting
-------------------------------------------------------------------------------------

--
-- unnest arrays on both levels in a sigle step
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


--
-- unnest arrays on both levels in two steps
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


--
-- display the total units sold on each product
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
	FROM flattened1),
units_sold AS (
	SELECT product_name, SUM (quantity) AS prod_units_sold
	FROM flattened2
	GROUP BY product_name
	)
SELECT *
FROM units_sold







-- ???????????????
-- two-level nesting with `jsonb_to_recordset`
SELECT
	id,
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	jsonb_array_elements(info -> 'orders')->> 'orderid' as orderid,
	jsonb_array_elements(info -> 'orders')->> 'orderdate' as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items,
	jsonb_array_elements(info -> 'orders' -> 'items')->> 'product' as product__wrong, -- NOT WORKING !!!
	jsonb_array_elements(jsonb_array_elements(info -> 'orders')-> 'items')->> 'product' AS product_name,
	jsonb_array_elements(jsonb_array_elements(info -> 'orders')-> 'items')->> 'qty' AS quantity,
	*
FROM orders2,
  jsonb_to_recordset(info -> 'orders' -> 'items') as (
  		orderid text, orderdate text, product text, qty text)
;


>>>


SELECT info ->> 'customer' AS customer, *
FROM orders,
  jsonb_to_recordset(info -> 'items') as (product varchar, qty numeric )


SELECT *
FROM orders,
  jsonb_array_elements(info -> 'items')

-- !!!
-- https://stackoverflow.com/questions/51045754/unnesting-a-list-of-json-objects-in-postgresql
SELECT id, info, info -> 'customer' as customer, y.key, y.value, x.record_number
FROM orders
   , lateral jsonb_array_elements(info -> 'items') WITH ORDINALITY AS x (val, record_number)
   , lateral jsonb_each_text(x.val) y


-- https://stackoverflow.com/questions/45807712/postgresql-aggregate-json-recordset-keys-by-row
SELECT *
FROM (
    SELECT id, info ->> 'customer' as customer,
        (jsonb_each(jsonb_array_elements(info ->'items'))).key as k,
       (jsonb_each(jsonb_array_elements(info ->'items'))).value::text as v
 			FROM orders
	) AS json_data


 */
