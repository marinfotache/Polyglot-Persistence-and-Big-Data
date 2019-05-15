//###################################################################################
//### 							Case Study: Sales in Cypher Neo4j
//###						            Cypher Queries
//###################################################################################

//###################################################################################
//###  Notice: Each of the following queries will be executed individually
//###    on the Neo4j browser (after opening the project abd starting the database)						          
//###################################################################################


//###################################################################################
//--    		Display all the nodes and all the relationships in the database

// The results will be hard to read
MATCH (n1)-[r]->(n2) RETURN n1, r, n2;



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
//--     Show the county name and the region for city of Pascani

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
//--			  Get all the postal codes for region of Moldova ?

MATCH (pc:PostalCode) -[rel1:PostalCodeInCounty]-> (c:County) -[rel2:CountyInRegion]-> (r:Region)
WHERE r.name = 'Moldova'
RETURN pc.post_code, pc.town AS town_name, c.name AS county_name, r.name AS region_name
ORDER BY pc.post_code



//###################################################################################
//--    				Display number of counties for each region
MATCH (c:County) -[rel:CountyInRegion]-> (r:Region)
RETURN r.name AS region_name, COUNT(*) AS n_of_counties
ORDER BY region_name


//###################################################################################
//--    					Display all the counties in each region

// function `COLLECT` provides a solution 
MATCH (c:County) -[rel:CountyInRegion]-> (r:Region)
RETURN r.name AS region_name, COLLECT(c.name) AS counties
ORDER BY region_name



//###################################################################################
//-- 					Get the overall number of invoices

// we'll count the number of nodes of type `:Invoice`
MATCH (:Invoice) 
RETURN COUNT(*) AS n_of_invoices



//###################################################################################
//-- 						Display of daily number of invoices 

//
MATCH (i:Invoice) 
RETURN i.invoice_date, COUNT(*) AS n_of_invoices
ORDER BY i.invoice_date


//###################################################################################
//-- 					Display invoices amount without VAT

// here we do not need information from `Products`
MATCH (i:Invoice) -[rel:InvoiceDetails]-> ()
RETURN i.invoice_id, i.invoice_date,
	SUM(rel.quantity * rel.unit_price) AS amount_without_VAT
ORDER BY i.invoice_id


	
//###################################################################################
// 								Display daily sales

// here we need information from `Products` (VAT percent)
MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
RETURN i.invoice_date as date,
	SUM(rel.quantity * rel.unit_price * (1 + p.current_vat_percent)) AS sales
ORDER BY i.invoice_date


//###################################################################################
// 				Display the list of the products sold on each day/date

// function COLLECT will be combined with a grouping operation
MATCH (i:Invoice) -[rel:InvoiceDetails]-> (p:Product)
RETURN i.invoice_date as date,
	COLLECT (p.product_name) AS product_list
ORDER BY i.invoice_date


 

//###################################################################################
//      	Which is the region with the highest number of counties ?
        
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
// 		Get, for each day of sales, the invoices with highest and the lowest amount 



//###################################################################################
// 							Display the yearly number of invoices 


//###################################################################################
// 				Get the number of invoices for each pair (year, month)


//###################################################################################
// 		Display number of invoices for each combination (year, month, day)




//###################################################################################
// 				Get the most frequently sold three products


//###################################################################################
// 		Get, for each invoice, three amounts: without VAT, VAT, amount with VAT 



//###################################################################################
// 				Get, for each invoice in September 2012, three amounts: 
//					without VAT, VAT, amount with VAT - sol. 1



//###################################################################################
// 			Get the amount received (paid by the client) for each invoice  
//				(result will include fully unpaid invoices)	



//###################################################################################
//  		Get the total amount and the paid amount for each invoice 
//
    	
//###################################################################################
// 			Which is the invoice with the greatest amount to be received 
//							(to be paid by the customer)?
	

















