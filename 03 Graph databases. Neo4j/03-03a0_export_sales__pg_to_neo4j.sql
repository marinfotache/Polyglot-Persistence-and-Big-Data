--############################################################################
--### 	          Export `sales` database from PostgreSQL to Neo4j
--############################################################################

--
-- I.
-- Connect to PgAdmin
-- create and populate database `sales` using scripts
-- see scripts `04-03a...` and `04-03b...` on github directory:
-- `https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/tree/master/04%20Graph%20databases.%20Neo4j`

-- as `sales` is the current database in pgAdmin IV, run the following statements

DROP TABLE IF EXISTS sales__pg_to_neo4j ;

CREATE TABLE sales__pg_to_neo4j AS
SELECT text FROM (
			SELECT * FROM (

	-- 1. Create `:Region` nodes
   SELECT DISTINCT 1 AS order_in_result,
   		'CREATE (region_' || LTRIM(RTRIM(region)) || ':Region {name:"' || region || '"})'
   		AS text
   	FROM (SELECT * FROM counties ORDER BY region )counties

	UNION

	-- 2. Link `:County` nodes to `:Region` nodes
   SELECT 2 AS order_in_result,
   		'CREATE (county_'|| LTRIM(RTRIM(countycode)) ||':County {code:"' || countycode || '", name:"' ||
					countyname || '"}) ' ||
			 ' -[:CountyInRegion]-> ' ||
			 ' (region_' || LTRIM(RTRIM(region)) || ') '
   		AS text
   	FROM (SELECT * FROM counties ORDER BY countyname ) counties

	UNION

	-- 3. Link `:PostalCode` nodes to `:County` nodes
   SELECT 3 AS order_in_result,
   		'CREATE (postcode_' || LTRIM(RTRIM(postcode))  || ':PostalCode {post_code:"' || postcode ||
					'", town:"' || place || '"}) ' ||
			 ' -[:PostalCodeInCounty]-> ' ||
			 ' (county_'|| LTRIM(RTRIM(countycode)) || ') '
   		AS text
   	FROM postcodes

	UNION

	-- 4. Create `:Category` nodes (Category names are composed of only letters, numbers and spaces)
   SELECT DISTINCT 4 AS order_in_result,
   		'CREATE (category_' || LTRIM(RTRIM(REPLACE(category, ' ', '_'))) || ':Category {category:"' || category || '"})'
   		AS text
   	FROM products

	UNION

	-- 5. Link `:Product` nodes to `:Category` nodes
   SELECT 5 AS order_in_result,
   		'CREATE (product_'|| productid ||':Product { product_id:' || productid || ', product_name:"' || productname ||
		     '", measure_unit:"' || unitofmeasurement ||
     		'", current_vat_percent: ' || vatpercent * 100 || ' }) ' ||
			 ' -[:ProductInCategory]-> ' ||
			' (category_' || LTRIM(RTRIM(REPLACE(category, ' ', '_'))) || ') '    AS text
   	FROM products

	UNION

	-- 6. Link `:Customer` nodes to `:PostalCode` nodes
	--   Notice: some attributes in table CUSTOMER are nullable
   SELECT 6 AS order_in_result,
		'CREATE (customer_' || customerid || ':Customer { cust_id:' || customerid ||
				COALESCE(', cust_name:"' || customername, '') ||
     			COALESCE('", fiscal_code:"' || fiscalcode, '') ||
				COALESCE('", cust_address:"' || address, '') ||
     			COALESCE('", cust_phone:"' || phone, '') || '"})'   ||
			 ' -[:LocatedIn]-> ' ||
			 ' (postcode_' || LTRIM(RTRIM(postcode)) || ') '
   		AS text
   	FROM customers

	UNION

	-- 7. Link `:Invoice` nodes to `:Customer` nodes
	--   Notice: `comments` attribute in table INVOICES is nullable and `invoicedare`
	--  is of type `date`
   SELECT 7 AS order_in_result,
		'CREATE (invoice_' || invoiceno || ':Invoice { invoice_id: ' || invoiceno ||
		    ', invoice_date: date("' || invoicedate || '")' ||
			COALESCE(', comments:"' || comments || '"', '') || ' }) ' ||
			' -[:SentTo]-> ' ||
			 ' (customer_' || customerid || ') '
	   		AS text
   	FROM invoices

	UNION

	-- 8. Link `:Invoice` nodes to `:Product` nodes
	--   Notice: attributes of table INVOICE_DETAILS will be transformed into
	-- properties of `:InvoiceDetails` relationships
   SELECT 8 AS order_in_result,
		'CREATE (invoice_' || invoiceno || ') '
			' -[ :InvoiceDetails {row_number: ' || invoicerownumber ||
			', quantity: ' || quantity || ', unit_price:' || unitprice ||
			'} ] -> ' ||
			 ' (product_' || productid || ') '
	   		AS text
   	FROM invoice_details

	UNION

	-- 9. Create `:Receipt` nodes
   SELECT 9 AS order_in_result,
		'CREATE (receipt_' || receiptid || ':Receipt { receipt_id: ' || receiptid ||
		    ', receipt_date: date("' || receiptdate || '")' ||
	    	', doc_type:"' || doctype || '", doc_number:"' || docnumber ||
		    '", doc_date: date("' || docdate || '")}) '
			AS text
   	FROM receipts

	UNION

	-- 10. Link `:Receipt` nodes to `:Invoice` nodes
   SELECT 10 AS order_in_result,
		'CREATE (receipt_' || receiptid || ') '
			' -[ :ReceiptPaysInvoice {amount: ' || amount || '} ] -> ' ||
			 ' (invoice_' || invoiceno || ')  '
	   		AS text
   	FROM receipt_details

			) x
			ORDER BY 1, 2
		) y
	;

SELECT * FROM sales__pg_to_neo4j ;





--############################################################################
-- II.
/* 						Next, in PgAdmin,
	1.a click on the table `sales__pg_to_neo4j`
	1.b righ-click, and choose `Import/Export`, then 'Export`
	1.c choose `Format` as `text` and save it a accessibile directory

The result must ressemble to the content of the file
`04-03b_export_sales_pg_to_neo4j.cypher`


--############################################################################
-- III.
/* 						Next, in  `Neo4j Desktop`
// 1. Launch Neo4j Desktop
// 2. Create a project called `sales`
// 3. Create a database (Local Graph) called `sales` (pay attention to the password)
// 5. Start (make active) the database `sales`
// 6. Open the Neo4j Browser


//##############################################################################
// In order to create the nodes, relationships and constraints, please
//  copy the entire content of the file/script resulted in step II
// (`04-03b_export_sales_pg_to_neo4j.cypher`), paste it into the
//  Neo4j Browser window, and run it.
//##############################################################################

*/
