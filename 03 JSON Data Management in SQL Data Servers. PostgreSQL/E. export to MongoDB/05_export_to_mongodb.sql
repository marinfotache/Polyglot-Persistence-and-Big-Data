--=========================================================================================
-- 						JSON Data Management in PostgreSQL
--=========================================================================================
-- 		05: for database `sales`, export previously created two JSON scenarios in
--    JSON files to be imported in two separate MongoDB databases (see section 5)
--=========================================================================================


--=========================================================================================
--           JSON `flat` scenario: export each PostgreSQL table as separate JSON file 
--                       to be imported with `mongoimport` utility
--=========================================================================================

DROP TABLE IF EXISTS  sales_json_flat ;

CREATE TABLE sales_json_flat (
	attrib varchar(5000)) ;

INSERT INTO sales_json_flat SELECT text FROM 
(SELECT 1 AS order, 'use sales_json_flat ;' AS text
	UNION
	SELECT 2, 'db.counties__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM counties__JSON_FLAT x 
	UNION
	SELECT 3, 'db.postcodes__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM postcodes__JSON_FLAT x 
	UNION
	SELECT 4, 'db.customers__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM customers__JSON_FLAT x 
	UNION
	SELECT 5, 'db.people__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM people__JSON_FLAT x 
	UNION
	SELECT 6, 'db.contacts__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM contacts__JSON_FLAT x 
	UNION
	SELECT 7, 'db.products__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM products__JSON_FLAT x 
	UNION
	SELECT 8, 'db.invoices__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM invoices__JSON_FLAT x 
	UNION
	SELECT 9, 'db.invoice_details__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM invoice_details__JSON_FLAT x 
	UNION
	SELECT 10, 'db.receipts__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM receipts__JSON_FLAT x 
	UNION
	SELECT 11, 'db.receipt_details__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM receipt_details__JSON_FLAT x 
	ORDER BY 1 ) t ;


SELECT * FROM sales_json_flat


/* Next, 

- in PgAdmin, 
	1. click on the table `sales_json_flat`
	2. righ-click, and choose `Import/Export`, then 'Export`
	3. choose `Format` as `text` and save it a accessibile directory
	
- connect to MongoDB with Mongoshell (in Terminal mode, type `mongo`)
- open the exported file, then `Select all` (Ctrl+A), then paste it into mongo shell window
		(alternatively Mongo import can be done with `load` command or `mongoimport` utility 
		(not covered here)
*/



--=========================================================================================
--           JSON `flat` scenario: export JSON file to be imported with `mongoimport`
--=========================================================================================

DROP TABLE IF EXISTS  sales_json_flat ;

CREATE TABLE sales_json_flat (
	attrib varchar(5000)) ;

INSERT INTO sales_json_flat SELECT text FROM 
(SELECT 1 AS order, 'use sales_json_flat ;' AS text
	UNION
	SELECT 2, 'db.counties__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM counties__JSON_FLAT x 
	UNION
	SELECT 3, 'db.postcodes__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM postcodes__JSON_FLAT x 
	UNION
	SELECT 4, 'db.customers__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM customers__JSON_FLAT x 
	UNION
	SELECT 5, 'db.people__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM people__JSON_FLAT x 
	UNION
	SELECT 6, 'db.contacts__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM contacts__JSON_FLAT x 
	UNION
	SELECT 7, 'db.products__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM products__JSON_FLAT x 
	UNION
	SELECT 8, 'db.invoices__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM invoices__JSON_FLAT x 
	UNION
	SELECT 9, 'db.invoice_details__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM invoice_details__JSON_FLAT x 
	UNION
	SELECT 10, 'db.receipts__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM receipts__JSON_FLAT x 
	UNION
	SELECT 11, 'db.receipt_details__JSON_FLAT.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM receipt_details__JSON_FLAT x 
	ORDER BY 1 ) t ;


SELECT * FROM sales_json_flat


/* Next, 

- in PgAdmin, 
	1. click on the table `sales_json_flat`
	2. righ-click, and choose `Import/Export`, then 'Export`
	3. choose `Format` as `text` and save it a accessibile directory
	
- connect to MongoDB with Mongoshell (in Terminal mode, type `mongo`)
- open the exported file, then `Select all` (Ctrl+A), then paste it into mongo shell window
		(alternatively to copy-paste in Mongo shell, in Mongo shell one can use `load` - see nect scenario

*/



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

DROP TABLE IF EXISTS  sales_json_nested ;
CREATE TABLE sales_json_nested (
	attrib varchar(5000)) ;

INSERT INTO sales_json_nested SELECT text FROM 
   (SELECT 1 AS order, 'use sales_json_nested ;' AS text
	UNION
	SELECT 2, 'db.counties__JSON_NESTED.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM counties__JSON_NESTED x 
	UNION
	SELECT 3, 'db.postcodes__JSON_NESTED.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM postcodes__JSON_NESTED x 
	UNION
	SELECT 4, 'db.customers__JSON_NESTED.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM customers__JSON_NESTED x 
	UNION
	SELECT 5, 'db.products__JSON_NESTED.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM products__JSON_NESTED x 
	UNION
	SELECT 6, 'db.invoices__JSON_NESTED.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM invoices__JSON_NESTED x 
	UNION
	SELECT 7, 'db.receipts__JSON_NESTED.insert (' || CAST (x.json_data AS TEXT) || ');'
	FROM receipts__JSON_NESTED x 
	ORDER BY 1 ) t ;

SELECT * FROM sales_json_nested ;	



/* Next, 

- in PgAdmin, 
	1. click on the table `sales_json_nested`
	2. righ-click, and choose `Import/Export`, then 'Export`
	3. choose `Format` as `text` and save it a accessibile directory 
	
- connect to MongoDB with mongoshell (in Terminal mode, type `mongo`)
- paste in mongoshell the content of the file exported from pgAdmin
*/




