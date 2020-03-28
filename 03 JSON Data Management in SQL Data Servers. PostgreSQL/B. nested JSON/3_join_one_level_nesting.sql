DROP TABLE IF EXISTS orders3 ;

CREATE TABLE orders3 (
 ID serial NOT NULL PRIMARY KEY,
 info jsonb NOT NULL
);


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
	 		{"product": "Toy Train","qty": 132}]}
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


/*

select * from orders2 

SELECT id,
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address
FROM orders2;


SELECT 
	id,
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders
FROM orders2;


-- one-level nesting
SELECT 
	id,
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	jsonb_array_elements(info -> 'orders')->> 'orderid' as orderid,
	jsonb_array_elements(info -> 'orders')->> 'orderdate' as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items
FROM orders2;



-- two-level nesting
SELECT 
	id,
	info ->> 'customer_name' AS customer_name,
	info ->> 'customer_address' AS customer_address,
	info ->> 'orders' AS customer_orders,
	jsonb_array_elements(info -> 'orders')->> 'orderid' as orderid,
	jsonb_array_elements(info -> 'orders')->> 'orderdate' as orderdate,
	jsonb_array_elements(info -> 'orders')->> 'items' as order_items,
	jsonb_array_elements(info -> 'orders' -> 'items')->> 'product' as product__wrong, -- NOT WORKING !!!
	jsonb_array_elements(jsonb_array_elements(info -> 'orders')-> 'items')->> 'product' AS product,
	jsonb_array_elements(jsonb_array_elements(info -> 'orders')-> 'items')->> 'qty' AS quantity
FROM orders2;



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


>>>>>>

select ('{ "x" : ["a", "b"]' || ',"y" :["c", "d"] }')::jsonb


SELECT info ->> 'customer' AS customer, *
FROM orders, 	
  jsonb_to_recordset(info -> 'items') as (product varchar, qty numeric )





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


 
