//###################################################################################
//###                      Case Study: Sales in Cypher Neo4j
//###            Cypher Queries (many requirements were used for MongoDB,
//###   so you can compare (in a certain extent) Cypher with Aggregation Framework)
//###################################################################################
//### last update: 2024-04-30

//###################################################################################
//###  Notice: Each of the following queries will be executed individually
//###    on the Neo4j browser (after opening the project and starting the database)
//###################################################################################


//###################################################################################
//--        Display all the nodes and all the relationships in the database

// The results will be hard to read
MATCH (n1)-[r]->(n2) RETURN n1, r, n2;

// Display the receipts for invoice 1111 (customer ('Client 1 SRL'))
MATCH (c:Customer) -[]- (i:Invoice) -[]- (r:Receipt) 
WHERE c.cust_name = 'Client 1 SRL' AND i.invoice_id = 1111  
RETURN *


// Display both the lines/products and the receipts, but only for invoice 1111 (customer ('Client 1 SRL'))
MATCH (n1) -[]- (n2) -[]- (n3) 
WHERE n1.cust_name = 'Client 1 SRL' AND n2.invoice_id = 1111  
RETURN *


//###################################################################################
//--    Display information about the county to which postal code '700505' belongs to

// only the object corresponding to the county where postal code '700505' belongs to
//   will be displayed (as single-node graph or JSON record)
MATCH (pc:PostalCode) -[r:PostalCodeInCounty]-> (c:County)
WHERE pc.post_code = '700505'
RETURN c

// both nodes and their relationship will be displayed (as graph or a record of JSON attributes )
MATCH (pc:PostalCode) -[r:PostalCodeInCounty]-> (c:County)
WHERE pc.post_code = '700505'
RETURN *


//###################################################################################
//--       Show the county name and the region for city of Pascani

// solution 1
MATCH (pc:PostalCode) -[rel:PostalCodeInCounty]-> (c:County) -[]-> (r:Region)
WHERE pc.town = 'Pascani'
RETURN pc.town AS town_name, c.name AS county_name, r.name AS region_name

// solution 2
MATCH (pc:PostalCode) -[]-> (c:County) -[]-> (r:Region)
WHERE pc.town = 'Pascani'
RETURN pc.town AS town_name, c.name AS county_name, r.name AS region_name

// solution 3
MATCH (pc:PostalCode) -[rel1:PostalCodeInCounty]-> (c:County) -[rel2:CountyInRegion]-> (r:Region)
WHERE pc.town = 'Pascani'
RETURN pc.town AS town_name, c.name AS county_name, r.name AS region_name


//###################################################################################
//--       Show the county name and the region for city of Iasi (two or more postal codes)

// solution 1
MATCH (pc:PostalCode) -[rel:PostalCodeInCounty]-> (c:County) -[]-> (r:Region)
WHERE pc.town = 'Iasi'
RETURN DISTINCT pc.town AS town_name, c.name AS county_name, r.name AS region_name

// solution 2
MATCH (pc:PostalCode) -[]-> (c:County) -[]-> (r:Region)
WHERE pc.town = 'Iasi'
RETURN DISTINCT pc.town AS town_name, c.name AS county_name, r.name AS region_name

// solution 3
MATCH (pc:PostalCode) -[rel1:PostalCodeInCounty]-> (c:County) -[rel2:CountyInRegion]-> (r:Region)
WHERE pc.town = 'Iasi'
RETURN DISTINCT pc.town AS town_name, c.name AS county_name, r.name AS region_name


//###################################################################################
//--             Get all the postal codes for region of Moldova ?

MATCH (pc:PostalCode) -[rel1:PostalCodeInCounty]-> (c:County) -[rel2:CountyInRegion]-> (r:Region)
WHERE r.name = 'Moldova'
RETURN pc.post_code, pc.town AS town_name, c.name AS county_name, r.name AS region_name
ORDER BY pc.post_code


//###################################################################################
//--            Display the products sold to customer "Client 1 SRL"
//###################################################################################
MATCH (cust_1:Customer { cust_name:"Client 1 SRL"})
WITH cust_1
MATCH (invoices_cust_1:Invoice) -[:SentTo]-> (cust_1)
WITH invoices_cust_1
MATCH (invoices_cust_1)  -[:InvoiceDetails] ->  (product_cust_1:Product)
RETURN DISTINCT product_cust_1.product_name AS prod_name
ORDER BY prod_name


//###################################################################################
//--    Extract the categories of the products sold in "Iasi" county 
//###################################################################################
//-- your turn!


//###################################################################################
//--       Display all products appearing in invoices along with `Product 1`
MATCH (p1:Product) <-[r1:InvoiceDetails]- (i:Invoice) -[r2:InvoiceDetails]-> (p2:Product)
WHERE p1.product_name = 'Product 1'
RETURN *


//###################################################################################
//--    Find products sold on both 2016-08-01 and 2016-08-02 dates
//###################################################################################

// sol. 1
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p:Product) <-[:InvoiceDetails]- 
	(i2:Invoice {invoice_date: date("2016-08-02")})
RETURN DISTINCT p.product_name

// sol. 2
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p1:Product)
WITH DISTINCT p1.product_name AS prod_names_date1
MATCH (i2:Invoice {invoice_date: date("2016-08-02")}) -[:InvoiceDetails] -> (p2:Product)
WHERE p2.product_name IN prod_names_date1
RETURN DISTINCT p2.product_name

// sol. 3
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p1:Product)
MATCH (i2:Invoice {invoice_date: date("2016-08-02")}) -[:InvoiceDetails] -> (p2:Product)
WHERE p1.product_name = p2.product_name
RETURN DISTINCT p2.product_name



//###################################################################################
//--    Find products sold to both "Client 1 SRL" and "Client 2 SA" customers
//###################################################################################
//-- your turn!


//###################################################################################
//--          Find products sold on 2016-08-01 but NOT ON 2016-08-02!
//###################################################################################

// Here, the result seems ok...
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p1:Product)
MATCH (i2:Invoice {invoice_date: date("2016-08-02")}) -[:InvoiceDetails] -> (p2:Product)
WHERE NOT p1.product_name IN p2.product_name
RETURN DISTINCT p1.product_name

// but when changing the order (Find products sold on 2016-08-02 but NOT ON 2016-08-01), 
//    the result is strange!
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p1:Product)
MATCH (i2:Invoice {invoice_date: date("2016-08-02")}) -[:InvoiceDetails] -> (p2:Product)
WHERE NOT p2.product_name IN p1.product_name
RETURN DISTINCT p2.product_name

// the correct solution:
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p1:Product)
MATCH (i2:Invoice {invoice_date: date("2016-08-02")}) -[:InvoiceDetails] -> (p2:Product)
WHERE NOT (p1)<-[:InvoiceDetails]-(i2)
RETURN DISTINCT p1.product_name

// this solution works when we change the order 
// (Find products sold on 2016-08-02 but NOT ON 2016-08-01)
MATCH (i1:Invoice {invoice_date: date("2016-08-01")}) -[:InvoiceDetails] -> (p1:Product)
MATCH (i2:Invoice {invoice_date: date("2016-08-02")}) -[:InvoiceDetails] -> (p2:Product)
WHERE NOT (p2)<-[:InvoiceDetails]-(i1)
RETURN DISTINCT p2.product_name


//###################################################################################
//-- Find products sold to customer "Client 1 SRL" but not to customer "Client 2 SA" 
//###################################################################################
//-- your turn!


//###################################################################################
//--               Display number of counties for each region
MATCH (c:County) -[rel:CountyInRegion]-> (r:Region)
RETURN r.name AS region_name, COUNT(*) AS n_of_counties
ORDER BY region_name


//###################################################################################
//--                       Display all the counties in each region

// function `COLLECT` provides a solution
MATCH (c:County) -[rel:CountyInRegion]-> (r:Region)
RETURN r.name AS region_name, COLLECT(c.name) AS counties
ORDER BY region_name


//###################################################################################
//--                 Display number of postal codes in each region
//-- your turn!


//###################################################################################
//--                        Get the overall number of invoices

// we count the number of nodes of type `:Invoice`
MATCH (:Invoice)
RETURN COUNT(*) AS n_of_invoices


//###################################################################################
//--                    Display of daily number of invoices

//
MATCH (i:Invoice)
RETURN i.invoice_date, COUNT(*) AS n_of_invoices
ORDER BY i.invoice_date


//###################################################################################
//-- 			Get the number of invoices in each region
//-- your turn!


//###################################################################################
//--                        Display invoices amount without VAT

// here we do not need information from `Products`
MATCH (i:Invoice) -[rel:InvoiceDetails]-> ()
RETURN i.invoice_id, i.invoice_date,
	SUM(rel.quantity * rel.unit_price) AS amount_without_VAT
ORDER BY i.invoice_id


//###################################################################################
//-- 						Get invoice amount with VAT
//-- your turn!


//###################################################################################
//                             Display the daily sales

// here we need information from `Products` (VAT percent)
MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
RETURN i.invoice_date as date,
	SUM(rel.quantity * rel.unit_price * (1 + p.current_vat_percent)) AS sales
ORDER BY i.invoice_date



//###################################################################################
//               Display the list of the products sold on each day/date

// function COLLECT is combined with a grouping operation
MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
RETURN i.invoice_date as date,
	COLLECT (p.product_name) AS product_list
ORDER BY i.invoice_date


//###################################################################################
//				For each date, get the number and the list of invoices, and
//               the number and the list of products

MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
RETURN i.invoice_date as date,
	COUNT(DISTINCT i.invoice_id) AS n_of_invoices,
	COLLECT (DISTINCT i.invoice_id) AS invoice_list,
	COUNT(DISTINCT p.product_id) AS n_of_prods,
	COLLECT (DISTINCT p.product_name) AS product_list
ORDER BY i.invoice_date


//###################################################################################
//                     Display the yearly number of invoices
MATCH (i:Invoice)
RETURN i.invoice_date.year as year,
	COUNT(*) AS n_of_invoices
ORDER BY i.invoice_date.year


//###################################################################################
//               Get the number of invoices for each pair (year, month)
MATCH (i:Invoice)
RETURN i.invoice_date.year as year, i.invoice_date.month as month,
	COUNT(*) AS n_of_invoices
ORDER BY year, month


//###################################################################################
//         Display number of invoices for each combination (year, month, day)
MATCH (i:Invoice)
RETURN
	i.invoice_date.year as year,
	i.invoice_date.month as month,
	i.invoice_date.day as day,
	COUNT(*) AS n_of_invoices
ORDER BY year, month, day


//###################################################################################
//      Get, for each invoice, three amounts: without VAT, VAT, amount with VAT

//
MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
RETURN i.invoice_id,  i.invoice_date as date,
	round(SUM(rel.quantity * rel.unit_price )) AS amount_without_VAT,
	round(SUM(rel.quantity * rel.unit_price * p.current_vat_percent / 100)) AS VAT,
	round(SUM(rel.quantity * rel.unit_price * (1 + p.current_vat_percent/ 100))) AS amount_with_VAT
ORDER BY i.invoice_id


//###################################################################################
//               Get, for each invoice issued in September 2016, three amounts:
//                  without VAT, VAT, amount with VAT

MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
WHERE i.invoice_date.year = 2016
RETURN i.invoice_id,  i.invoice_date as date,
	round(SUM(rel.quantity * rel.unit_price )) AS amount_without_VAT,
	round(SUM(rel.quantity * rel.unit_price * p.current_vat_percent / 100)) AS VAT,
	round(SUM(rel.quantity * rel.unit_price * (1 + p.current_vat_percent/ 100))) AS amount_with_VAT
ORDER BY i.invoice_id


//###################################################################################
// 						Get average invoice value (amount)

MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
WITH i.invoice_id AS invoice_id,  
	round(SUM(rel.quantity * rel.unit_price * (1 + p.current_vat_percent/ 100))) AS amount_with_VAT
RETURN AVG(amount_with_VAT)


//###################################################################################
// 				Get average invoice amount for each day 
//-- your turn!


//###################################################################################
//          Get the amount received (paid by the client) for each invoice
//               (the result will include fully unpaid invoices)

// `OPTIONAL` is needed for including in the result all the invoices
MATCH  (i:Invoice)
OPTIONAL MATCH (i)  <-[rel:ReceiptPaysInvoice]- (r:Receipt)
RETURN i, sum(rel.amount) AS total_received
ORDER BY i.invoice_id



//###################################################################################
//          Get the total amount and the paid amount for each invoice

//
MATCH  (i:Invoice) -[rel1:InvoiceDetails]-> (p:Product)
WITH i, round(SUM(rel1.quantity * rel1.unit_price *
        	(1 + p.current_vat_percent/ 100))) AS amount_with_VAT
WITH i, amount_with_VAT
OPTIONAL MATCH (i)  <-[rel2:ReceiptPaysInvoice]- (r:Receipt)
RETURN i, amount_with_VAT,
		sum(rel2.amount) AS total_received
ORDER BY i.invoice_id


//###################################################################################
// 		  Display all invoices issued to the same customer as for invoice 1111

MATCH (i1111:Invoice {invoice_id : 1111}) -[:SentTo]-> (cust_inv1111:Customer) <- [:SentTo]- (i:Invoice)
RETURN i.invoice_id
ORDER BY i.invoice_id


//###################################################################################
// 	Display all invoices issued to customers located at the same postal code
//      as the zip code of the customer for invoice 1111
//-- your turn!


//###################################################################################
// 					Display invoices issued in the first sales date

// sol.1
MATCH  (i:Invoice)
WITH MIN(i.invoice_date) AS first_date
MATCH  (i:Invoice {invoice_date: first_date})
RETURN *
ORDER BY i.invoice_id

// sol.2
MATCH  (i:Invoice)
WITH MIN(i.invoice_date) AS first_date
MATCH  (i:Invoice)
WHERE i.invoice_date = first_date
RETURN *
ORDER BY i.invoice_id


//###################################################################################
// 						Display invoices issued in the first 7 days of sales
MATCH  (i:Invoice)
WITH MIN(i.invoice_date) AS first_date
MATCH  (i:Invoice)
WHERE i.invoice_date <= first_date + Duration({days: 7})
RETURN *
ORDER BY i.invoice_date, i.invoice_id


//###################################################################################
//   Get, for each day of sales, the invoices with highest and the lowest amounts

MATCH  (i:Invoice) -[rel1:InvoiceDetails]-> (p:Product)
WITH i, round(SUM(rel1.quantity * rel1.unit_price *
        	(1 + p.current_vat_percent/ 100))) AS amount_with_VAT
WITH i.invoice_date AS date, i.invoice_id AS invoice_id, amount_with_VAT
WITH date, max(amount_with_VAT) AS max_invoice, min(amount_with_VAT) AS min_invoice
WITH date, max_invoice, min_invoice
MATCH (i:Invoice) -[rel1:InvoiceDetails]-> (p:Product)
WITH date, max_invoice, min_invoice, i, round(SUM(rel1.quantity * rel1.unit_price *
        	(1 + p.current_vat_percent/ 100))) AS amount_with_VAT
WHERE i.invoice_date = date AND amount_with_VAT IN [max_invoice, min_invoice] 		
RETURN *
ORDER BY i.invoice_date, amount_with_VAT DESC


//###################################################################################
//                Which is the region with the highest number of counties ?

// a solution which does not display the ties
MATCH (c:County) -[:CountyInRegion]-> (r:Region)
WITH r.name AS region_name, COUNT(*) AS n_of_counties
RETURN *
ORDER BY n_of_counties DESC
LIMIT 1

// a complete  solution (takes into account the ties)
MATCH (c:County) -[:CountyInRegion]-> (r:Region)
WITH r.name AS region_name, COUNT(*) AS n_of_counties
WITH region_name, n_of_counties
WITH MAX(n_of_counties) AS max_n_of_counties
MATCH (c:County) -[:CountyInRegion]-> (r:Region)
WITH max_n_of_counties, r.name AS region_name, COUNT(*) AS n_of_counties
WHERE n_of_counties = max_n_of_counties
RETURN *


//###################################################################################
//      Which is the invoice with the greatest amount to be received
//							(to be paid by the customer)?


// next solution extract both invoices that have the same amount to be received
MATCH  (i:Invoice) -[rel1:InvoiceDetails]-> (p:Product)
WITH i, round(SUM(rel1.quantity * rel1.unit_price *
        	(1 + p.current_vat_percent/ 100))) AS amount_with_VAT
WITH i, amount_with_VAT
OPTIONAL MATCH (i)  <-[rel2:ReceiptPaysInvoice]- (r:Receipt)
WITH i, amount_with_VAT, sum(rel2.amount) AS paid
WITH i, amount_with_VAT -  paid AS to_be_received
WITH  MAX(to_be_received) AS max_to_be_received
MATCH  (i:Invoice) -[rel1:InvoiceDetails]-> (p:Product)
WITH max_to_be_received, i, round(SUM(rel1.quantity * rel1.unit_price *
        	(1 + p.current_vat_percent/ 100))) AS amount_with_VAT
WITH max_to_be_received, i, amount_with_VAT
OPTIONAL MATCH (i)  <-[rel2:ReceiptPaysInvoice]- (r:Receipt)
WITH max_to_be_received, i, amount_with_VAT, sum(rel2.amount) AS paid
WITH max_to_be_received, i, amount_with_VAT -  paid AS to_be_received
WHERE max_to_be_received = to_be_received
RETURN *


//###################################################################################
//                     Get the most frequently sold three products
//-- your turn!


//###################################################################################
// Extract customers with at least the number of invoices of customer "Client 5 SRL"
//-- your turn!


//###################################################################################
//  Extract customers with the sales amount greater than or equal to
//   the customer "Client 5 SRL"
//-- your turn!
