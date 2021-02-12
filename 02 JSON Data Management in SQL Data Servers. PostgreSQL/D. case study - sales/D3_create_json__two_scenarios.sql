--=========================================================================================
-- 									 JSON Data Management in PostgreSQL
--=========================================================================================
-- 		D3. For database `sales`, within the same (sub)schema, two series of JSON tables
-- 					will be created,  `flat` JSON, and `nested` JSON
--=========================================================================================


--=========================================================================================
--                      					JSON `flat` scenario:
-- for each normalized table, a table will be created with just two attributes, an
-- `id` (autoincremented) and `json_data` with all attributes taken from the
-- relational tables; no foreign keys will be declared, but only primary key and some
-- alternate keys and not null restrictions
--=========================================================================================




-- counties__JSON_FLAT
DROP TABLE IF EXISTS counties__JSON_FLAT ;
CREATE TABLE counties__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM counties x ;
ALTER TABLE counties__JSON_FLAT ADD PRIMARY KEY (id) ;

-- declare an alternate key for a JSON sub-attribute
CREATE UNIQUE INDEX counties__JSON_FLAT_pk ON counties__JSON_FLAT ((json_data ->> 'countycode')) ;

-- declare a not null constraint for a JSON sub-attribute
ALTER TABLE counties__JSON_FLAT
	ADD CONSTRAINT counties__JSON_FLAT_countyname CHECK ((json_data ->> 'countyname') IS NOT NULL);


SELECT * FROM counties__JSON_FLAT ;


-- postcodes__JSON_FLAT
DROP TABLE IF EXISTS postcodes__JSON_FLAT ;
CREATE TABLE postcodes__JSON_FLAT AS
	SELECT row_number() over () AS id, to_jsonb(x) AS json_data
	FROM postcodes x ;
ALTER TABLE postcodes__JSON_FLAT ADD PRIMARY KEY (id) ;

SELECT * FROM postcodes__JSON_FLAT ;


-- customers__JSON_FLAT (notice `jsonb_strip_nulls`)
DROP TABLE IF EXISTS customers__JSON_FLAT ;
CREATE TABLE customers__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM customers x ;
ALTER TABLE customers__JSON_FLAT ADD PRIMARY KEY (id) ;


-- people__JSON_FLAT
DROP TABLE IF EXISTS people__JSON_FLAT ;
CREATE TABLE people__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM people x ;
ALTER TABLE people__JSON_FLAT ADD PRIMARY KEY (id) ;


-- contacts__JSON_FLAT
DROP TABLE IF EXISTS contacts__JSON_FLAT ;
CREATE TABLE contacts__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM contacts x ;
ALTER TABLE contacts__JSON_FLAT ADD PRIMARY KEY (id) ;


-- products__JSON_FLAT
DROP TABLE IF EXISTS products__JSON_FLAT ;
CREATE TABLE products__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM products x ;
ALTER TABLE products__JSON_FLAT ADD PRIMARY KEY (id) ;


-- invoices__JSON_FLAT
DROP TABLE IF EXISTS invoices__JSON_FLAT ;
CREATE TABLE invoices__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM invoices x ;
ALTER TABLE invoices__JSON_FLAT ADD PRIMARY KEY (id) ;


-- invoice_details__JSON_FLAT
DROP TABLE IF EXISTS invoice_details__JSON_FLAT ;
CREATE TABLE invoice_details__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM invoice_details x ;
ALTER TABLE invoice_details__JSON_FLAT ADD PRIMARY KEY (id) ;


-- receipts__JSON_FLAT
DROP TABLE IF EXISTS receipts__JSON_FLAT ;
CREATE TABLE receipts__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM receipts x ;
ALTER TABLE receipts__JSON_FLAT ADD PRIMARY KEY (id) ;


-- receipt_details__JSON_FLAT
DROP TABLE IF EXISTS receipt_details__JSON_FLAT ;
CREATE TABLE receipt_details__JSON_FLAT AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM receipt_details x ;
ALTER TABLE receipt_details__JSON_FLAT ADD PRIMARY KEY (id) ;




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
-- `counties__JSON_NESTED` is similar to `counties__JSON_FLAT`
DROP TABLE IF EXISTS counties__JSON_NESTED ;
CREATE TABLE counties__JSON_NESTED AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM counties x ;
ALTER TABLE counties__JSON_NESTED ADD PRIMARY KEY (id) ;

-- declare an alternate key for a JSON sub-attribute
CREATE UNIQUE INDEX counties__JSON_NESTED_pk ON counties__JSON_NESTED ((json_data ->> 'countycode')) ;

-- declare a not null constraint for a JSON sub-attribute
ALTER TABLE counties__JSON_NESTED
	ADD CONSTRAINT countyname CHECK ((json_data ->> 'countyname') IS NOT NULL);

SELECT * FROM counties__JSON_NESTED ;



--
-- also `postcodes__JSON_NESTED` is similar to `postcodes__JSON_FLAT`
DROP TABLE IF EXISTS postcodes__JSON_NESTED ;
CREATE TABLE postcodes__JSON_NESTED AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM postcodes x ;
ALTER TABLE postcodes__JSON_NESTED ADD PRIMARY KEY (id) ;

SELECT * FROM postcodes__JSON_NESTED ;



--
--
-- `customers__JSON_NESTED` is much more interesting, since it is based
-- on `customers` + `contacts` + `people`  tables in the normalized schema

-- first, we fusion `contacts` with `people`
DROP TABLE IF EXISTS new_contacts ;
CREATE TABLE new_contacts AS
	SELECT customerid, position,
		jsonb_strip_nulls(to_jsonb(people)) AS person
	FROM contacts NATURAL JOIN people ;

-- examine the newly created table
SELECT * FROM new_contacts ;

-- now, we'll nest all contacts of each customer
DROP TABLE IF EXISTS customers__JSON_NESTED ;
CREATE TABLE customers__JSON_NESTED AS
	SELECT row_number() OVER () AS id,
		jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM (
		SELECT customers.*,
			(SELECT to_jsonb(array_agg(jsonb_strip_nulls(to_jsonb(y))))     -- notice `to_jsonb(array_agg(to_jsonb(y)))`
			 FROM (
				SELECT new_contacts.person, new_contacts.position
				FROM new_contacts
				WHERE customerid = customers.customerid
				) y
			 ) AS new_contacts
		FROM customers) x ;

ALTER TABLE customers__JSON_NESTED ADD PRIMARY KEY (id) ;

-- examine
SELECT * FROM customers__JSON_NESTED ;

-- drop the intermediary result
DROP TABLE IF EXISTS new_contacts ;



--
-- `products__JSON_NESTED` is similar to `products__JSON_FLAT`
DROP TABLE IF EXISTS products__JSON_NESTED ;
CREATE TABLE products__JSON_NESTED AS
	SELECT row_number() over () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM products x ;
ALTER TABLE products__JSON_NESTED ADD PRIMARY KEY (id) ;




--
-- `invoices__JSON_NESTED` is based on `invoices` + `invoice_details` + `products`  tables in the normalized schema
--

-- first, we fusion `invoice_details` with `products`
DROP TABLE IF EXISTS new_invoice_details ;
CREATE TABLE new_invoice_details AS
	SELECT invoiceno, invoicerownumber,
		jsonb_strip_nulls(to_jsonb(products)) AS product,
		quantity, unitprice
	FROM invoice_details NATURAL JOIN products ;

-- examine the newly created table
SELECT * FROM new_invoice_details ;


-- now, we'll nest all lines of each invoice
DROP TABLE IF EXISTS invoices__JSON_NESTED ;
CREATE TABLE invoices__JSON_NESTED AS
	SELECT row_number() OVER () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM (
		SELECT invoices.*,
			(SELECT jsonb_strip_nulls(to_jsonb(array_agg(to_jsonb(y))))     -- notice `to_jsonb(array_agg(to_jsonb(y)))`
			FROM (
				SELECT invoicerownumber, product, quantity, unitprice
				FROM new_invoice_details
				WHERE invoiceno = invoices.invoiceno
				) y
			) AS invoice_details
		FROM invoices) x ;

-- examine the table
SELECT * FROM invoices__JSON_NESTED	;


-- drop the intermediary table
DROP TABLE IF EXISTS new_invoice_details ;



--
-- `receipts__JSON_NESTED` is based on `receipts` + `receipt_details` tables in the normalized schema
--
SELECT * FROM receipt_details ;

DROP TABLE IF EXISTS receipts__JSON_NESTED ;
CREATE TABLE receipts__JSON_NESTED AS
	SELECT row_number() OVER () AS id, jsonb_strip_nulls(to_jsonb(x)) AS json_data
	FROM (
		SELECT receipts.*,
			(SELECT jsonb_strip_nulls(to_jsonb(array_agg(to_jsonb(y))))     -- notice `to_jsonb(array_agg(to_jsonb(y)))`
			FROM (
				SELECT invoiceno, amount
				FROM receipt_details
				WHERE receiptid = receipts.receiptid
				) y
			) AS receipt_details
		FROM receipts) x ;


-- examine the table
SELECT * FROM receipts__JSON_NESTED	;
