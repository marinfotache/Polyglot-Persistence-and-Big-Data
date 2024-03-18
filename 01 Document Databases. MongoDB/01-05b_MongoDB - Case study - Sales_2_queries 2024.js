//===============================================================================
//                           Case study:  SALES in MongoDB
//	 Part 2: Query database collections using (mainly) Aggregation Framework
//===============================================================================
// last update: 2024-03-18


//==================================================================================
//           Some useful commands in Terminal (Mongo Shell) mode
//==================================================================================

//--   show databases on the server
show dbs

//-- show current database
db

//--   select current database
use sales

//--   list all colections in current database
show collections


db.counties.find() ;
db.postalCodes.find() ;
db.customers.find().pretty() ;
db.products.find().pretty() ;
db.invoices.find().pretty() ;
db.receipts.find().pretty() ;


//==================================================================================
//                           
//	      !!!  Before proceesing with commands in this scripy, do not forget 
//        to run the previous script (01-05a...) for creating the collections   !!!
//
//==================================================================================


//==================================================================================
//                   I.  Basic queries (data retrieval, aggregation)
//      Some previously discussed solutions (script 01-04...)
//==================================================================================

//----------------------------------------------------------------------------------
//-- Display information about the county to which postal code '700505' belongs to
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// one of the the simplest solutions (with `findOne`):
var row_codpost = db.postalCodes.findOne ({_id : '700505'}) ;
db.counties.find({'_id' : row_codpost['countyCode']}) ;

//----------------------------------------------------------------------------------
// a solution based on `find`...`toArray`:
var myArray = db.postalCodes.find({_id : '700505'}).toArray() ;
db.counties.find({'_id' : myArray[0].countyCode'}) ;


//----------------------------------------------------------------------------------
// another one based on a cursor & hasNext
var myCursor = db.postalCodes.find ({_id : '700505'}) ;
var myRow = myCursor.hasNext() ? myCursor.next() :null ;
if (myRow) {
	var myCountyCode = myRow.countyCode ;
	print (myCountyCode) ;
	}
db.counties.find({'_id' : myCountyCode }) ;

//----------------------------------------------------------------------------------
//  another one based on a cursor & forEach
var myCursor = db.postalCodes.find ({_id : '700505'}) ;
var  myCountyCode ;
myCursor.forEach(function(x) {
	myCountyCode = x.countyCode ;
	print(myCountyCode) ;
	} ) ;
db.counties.find({'_id' : myCountyCode }) ;

//----------------------------------------------------------------------------------
//--    A solution based on Aggregation Framework and operator "$out"

// db.adminCommand( { setFeatureCompatibilityVersion: "4.4" } )

db.postalCodes.aggregate( [
	{ $match: { _id : "700505" }},
	{ $project : { countyCode :1, _id: 0} },
	{ $out: "region_zip"}
	] ) ;
// use the newly created collection	as a parameter for searching in "counties"
db.counties.find({'_id' :(db.region_zip.findOne()).countyCode }) ;


//----------------------------------------------------------------------------------
//--    A new solution based on Aggegation Framework and pseudo-join
// It is not recommended in this case, but we can get the idea

// clean up the collection containing the result
db.result.remove({}) ;

// aggregate returns a cursor which will be processed row by row
db.postalCodes.aggregate([
	{ $match: {_id : "700505"}}]).forEach(function(pc) {
	var county = db.counties.findOne({_id: pc.countyCode});
        if (county !== null) {
        		pc.countyName = county.countyName;
            pc.countyRegion = county.countyRegion;
        } else {
            	pc.countyName = "not found";
              pc.countyRegion = "not found";
        }
        db.result.insertOne(pc)
}   ) ;

// display collection results
db.result.find();


//--------------------------------------------------------------------------------
// Starting with MongoDB version 3.2: $lookup - which emulates a left outer join
//    of two collections in the same database

// first, a left join of `counties` with `postalCodes`
db.counties.aggregate([
    {
      $lookup:
        {
          from: "postalCodes",
          localField: "_id",
          foreignField: "countyCode",
          as: "counties__post_codes"
        }
   }
]);

// now, a left join of  `postalCodes` with `counties`
db.postalCodes.aggregate([
    {
      $lookup:
        {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "post_codes__counties"
        }
   }
]);


//--------------------------------------------------------------------------------
// here is the solution (find the county the postal code '700505' belongs to
db.postalCodes.aggregate([
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "post_codes__counties" } },
    { $match: { _id : "700505"   } },
    { $project: {post_codes__counties : 1} }
]);

//--------------------------------------------------------------------------------
// solution based on the lookup where the sttarting collection is `counties`
db.counties.aggregate([
    { $lookup: {
          from: "postalCodes",
          localField: "_id",
          foreignField: "countyCode",
          as: "counties__pc" } },
    { $match: { "counties__pc._id" : "700505"   } }, 
    { $project: {counties__pc : 0} }
]);


//--------------------------------------------------------------------------------
// ... it is advisable to filter the records as soon as possible
db.postalCodes.aggregate([
    { $match: { _id : "700505"   } },
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "post_codes__counties" } },
    { $project: {post_codes__counties : 1} }
]);


//--------------------------------------------------------------------------------
// something more challenging: `lookup` - `let` - `pipeline`
db.counties.aggregate([
	{ $lookup : {
			from : "postalCodes",
			let : { countyCode_ : "$_id"},
			pipeline : [
    					{ $match: { $expr : { $and : [
									{ $eq : [ "$_id", "700505"  ] },
									{ $eq : [  "$$countyCode_", "$countyCode"] }
							   ]} } }
					],
			as : "postal_code"
	}  },
	{ $match : { $expr : { $gt : [ { $size : "$postal_code"}, 0 ] }}}
]) ;


//----------------------------------------------------------------------------------
//-- Display only the county name to which postal code '700505' belongs to
//----------------------------------------------------------------------------------

db.postalCodes.aggregate([
    { $match: { _id : "700505"   } },
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "post_codes__counties" } },
    { $project: {county_name : "$post_codes__counties.countyName", _id : 0} }
]);



//----------------------------------------------------------------------------------
//--     		 Show the county name and the region for city of Pascani
//----------------------------------------------------------------------------------

//--------------------------------------------------------------------------------
// Store the results of "find" into year_ array variable ("myArray")
var myArray = db.postalCodes.find({cityName : 'Pascani'}).toArray() ;
db.counties.find({'_id' : myArray[0].countyCode'}) ;


//--------------------------------------------------------------------------------
// with $lookup (left join)
db.postalCodes.aggregate([
	{ $match: { cityName : 'Pascani'  }},
	{ $limit: 1},
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "cities__counties" } },
    { $project: {cities__counties : 1} }
]) 

//--------------------------------------------------------------------------------
// with `lookup` - `let` - `pipeline`
db.counties.aggregate([
	{ $lookup : {
			from : "postalCodes",
			let : { countyCode_ : "$_id"},
			pipeline : [
    					{ $match: { $expr : { $and : [
									{ $eq : [ "$cityName", "Pascani"  ] },
									{ $eq : [  "$$countyCode_", "$countyCode"] }
							   ]} } }
					],
			as : "postal_code"
	}  },
	{ $match : { $expr : { $gt : [ { $size : "$postal_code"}, 0 ] }}}
]) ;



//----------------------------------------------------------------------------------
//             Get all the postal codes for region of Moldova
//----------------------------------------------------------------------------------

//--------------------------------------------------------------------------------
//  a solution using regular expressions
var myCursor = db.counties.find ({countyRegion  : 'Moldova'}) ;
var myRegExp = "";
myCursor.forEach(function(x) {
	var myCountyCode = x._id ;
	if (myRegExp == "") {
		myRegExp = "^" + myCountyCode ; }
	else {
		myRegExp = myRegExp + "|^" + myCountyCode ; }
	} ) ;
db.postalCodes.find({'countyCode' : {"$regex" : myRegExp  } }) ;


//--------------------------------------------------------------------------------
// pseudo-join
db.result.remove({}) ;
db.counties.aggregate({ $match: {countyRegion  : 'Moldova'}}).forEach(function(pc) {	
    var postal_codes = db.postalCodes.find({countyCode: pc._id}).toArray() ;
    if (postal_codes.length > 0) {
        pc.postalCodes = postal_codes;
    } else {
        pc.postalCodes = "not found";
    }
    db.result.insert(pc)
}   ) ;
// display collection results
db.result.find().pretty() ;


//--------------------------------------------------------------------------------
// $lookup (left join)
db.postalCodes.aggregate([
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "post_codes__counties" } },
    { $match: { "post_codes__counties.countyRegion"  : 'Moldova'    } }
])

// ...or
db.counties.aggregate([
	{ $match: { countyRegion  : 'Moldova'    } },
    { $lookup: {
          from: "postalCodes",
          localField: "_id",
          foreignField: "countyCode",
          as: "counties__post_codes" } }
])

// ...or (better formatted) - NOTICE $unwind !!!
db.counties.aggregate([
    { $match : {"countyRegion" : "Moldova" }},
    { $lookup: {
          from: "postalCodes",
          localField: "_id",
          foreignField: "countyCode",
          as: "c_pc" } },
     { $unwind : "$c_pc"},
     { $project : {postal_code : "$c_pc._id", loc : "$c_pc.cityName", _id : 0, countyName : 1 }}
])


//--------------------------------------------------------------------------------
// with `lookup` - `let` - `pipeline`
db.postalCodes.aggregate([
	{ $lookup : {
			from : "counties",
			let : { countyCode_ : "$countyCode"},
			pipeline : [
    					{ $match: { $expr : { $and : [
									{ $eq : [ "$countyRegion", "Moldova"  ] },
									{ $eq : [  "$$countyCode_", "$_id"] }
							   ]} } }
					],
			as : "county"
	}  },
	{ $match : { $expr : { $gt : [ { $size : "$county"}, 0 ] }}},
	//{ $project : {county : 0}}	
]) ;


//----------------------------------------------------------------------------------
//--            Display the products sold to customer "Client 1 SRL"
//----------------------------------------------------------------------------------
// to be written during lecture
//...

// sol. 1
db.customers.aggregate([
	{ $match: { custName  : 'Client 1 SRL'    } },
	{ $project : {contacts : 0} },
    { $lookup: {
          from: "invoices",
          localField: "_id",
          foreignField: "custID",
          as: "cust_inv" } },
	{ $project : {_id:0,  custName: 1, prod_name : "$cust_inv.items.product.prodName" } },
])


// sol. 2 - a more elegant result format 
db.customers.aggregate([
	{ $match: { custName  : 'Client 1 SRL'    } },
	{ $project : {contacts : 0} },
    { $lookup: {
          from: "invoices",
          localField: "_id",
          foreignField: "custID",
          as: "cust_inv" } },
    { $unwind : "$cust_inv"},
    { $unwind : "$cust_inv.items"},
	{ $project : {_id:0,  prod_name : "$cust_inv.items.product.prodName" } },
    { $group: { _id: "$prod_name"}},
    { $sort: {_id: 1}}
])



//----------------------------------------------------------------------------------
//--                 Display number of counties for each region
//----------------------------------------------------------------------------------

db.counties.aggregate([
    { $group: { _id: {region: "$countyRegion"}, n_of_counties: { $sum: 1} } },
     { $sort: {_id: 1}}
	 ]);


//----------------------------------------------------------------------------------
//--                   Display all the counties of each region
//----------------------------------------------------------------------------------

db.counties.aggregate([
    { $group: { _id: {region: "$countyRegion"}, n_of_counties: { $sum: 1},
       counties: { $addToSet: "$countyName"} } },
     { $sort: {_id: 1}}
	 ]);


//----------------------------------------------------------------------------------
//--                 Display number of postal codes in each region
//----------------------------------------------------------------------------------
// to be written during lecture
//...

// sol. 1

//----------------------------------------------------------------------------------
db.counties.aggregate([
    { $lookup: {
          from: "postalCodes",
          localField: "_id",
          foreignField: "countyCode",
          as: "counties__post_codes" } },
    { $unwind : "$counties__post_codes"},      
    { $group: { _id: {region: "$countyRegion"}, n_of_counties: { $sum: 1} } }        
])


//----------------------------------------------------------------------------------
// sol. 2
db.postalCodes.aggregate([
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "pc_counties" } },
    { $group: { _id: {region: "$pc_counties.countyRegion"}, n_of_counties: { $sum: 1} } }        
])


//----------------------------------------------------------------------------------
// sol. 3
db.postalCodes.aggregate([
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "pc_counties" } },
    { $unwind : "$pc_counties" },      
    { $group: { _id: {region: "$pc_counties.countyRegion"}, n_of_counties: { $sum: 1} } }        
])


//----------------------------------------------------------------------------------
//--                 Display the list of postal codes in each region
//----------------------------------------------------------------------------------
// to be written during lecture
//...

// 
db.postalCodes.aggregate([
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "pc_counties" } },
    { $unwind : "$pc_counties" },      
    { $group: { _id: {region: "$pc_counties.countyRegion"}, n_of_counties: { $sum: 1}, 
               postal_codes: { $addToSet: "$pc_counties.countyName"}} }     
])


//----------------------------------------------------------------------------------
//-- 				Get the overall number of invoices
//----------------------------------------------------------------------------------

db.invoices.aggregate([
	{ $group: { _id: null, n_of_invoices : { $sum : 1 } } }
	]) ;


//----------------------------------------------------------------------------------
//-- 				Get the number of daily invoices
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
db.invoices.aggregate  ([
	{ $group: { _id: "$invDate", n_of_invoices : { $sum : 1 } } }
	] ) ;


//----------------------------------------------------------------------------------
//-- number of daily invoices ordered by dates
db.invoices.aggregate  ([
	{ $group: { _id: "$invDate", n_of_invoices : { $sum : 1 } } },
	{ $sort : {_id : 1} }
	] ) ;



//----------------------------------------------------------------------------------
//-- 			Get the number of invoices in each region
//----------------------------------------------------------------------------------
// to be written during lecture
//...

db.invoices.aggregate([
    { $project : { custID : 1}} ,
    { $lookup: {
          from: "customers",
          localField: "custID",
          foreignField: "_id",
          as: "inv_cust" } },
    { $project : { customer_name : "$inv_cust.custName", postal_code : "$inv_cust.postCode"}} ,
    { $lookup: {
          from: "postalCodes",
          localField: "postal_code",
          foreignField: "_id",
          as: "inv_cust_pc" } },
    { $project : { customer_name : 1, postal_code : 1, county_code : "$inv_cust_pc.countyCode"}} ,
    { $lookup: {
          from: "counties",
          localField: "county_code",
          foreignField: "_id",
          as: "inv_cust_pc_c" } },
    { $group: { _id: {region: "$inv_cust_pc_c.countyRegion"}, n_of_counties: { $sum: 1} } }    
])


	


//----------------------------------------------------------------------------------
//-- 						     Get invoice amount without VAT
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// sol. 1
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", amount_without_VAT :
				{ $sum : {$multiply : ["$items.quantity", "$items.unitPrice" ] } } } },
	{ $sort : {_id : 1} }
	] ) ;


//----------------------------------------------------------------------------------
	// sol. 2 - with `$addFields`
db.invoices.aggregate( [
	{ $unwind : "$items" },
  { $addFields : { line_amount : { $multiply: ["$items.quantity", "$items.unitPrice" ] } } },
	{ $group : { _id : "$_id", amount_without_VAT : { $sum : "$line_amount" } } },
	{ $sort : {_id : 1} }
	] ) ;


//----------------------------------------------------------------------------------
// 								Get invoice amount with VAT
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// sol.1
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] },
				] } } } } ,
	{ $sort : {_id : 1} }
	] ) ;

//----------------------------------------------------------------------------------
// sol. 2 - with `$addFields`
db.invoices.aggregate( [
	{ $unwind : "$items" },
  { $addFields : { line_amount : { $multiply: ["$items.quantity", "$items.unitPrice" ] } } },
  { $addFields : { line_vat : { $multiply: ["$line_amount", "$items.product.percVAT" ] } } },
	{ $addFields : { line_with_vat : { $add: ["$line_amount", "$line_vat" ] } } },
	{ $group : { _id : "$_id", invoice_amount : { $sum : "$line_with_vat"  } } },
	{ $sort : {_id : 1} }
	] ) ;



//----------------------------------------------------------------------------------
// 									  Extract daily sales
//----------------------------------------------------------------------------------

db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$invDate", daily_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : {_id : 1} }
	] ) ;


//----------------------------------------------------------------------------------
//				Something different from "traditional" SQL: daily (sold) products
//----------------------------------------------------------------------------------

db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$invDate",
			n_of_inv_lines : { $sum : 1},
			sold_products : { $addToSet : "$items.product.prodName" } }},
  	{ $addFields : { n_of_prods : { $size: "$sold_products"} } },
	{ $sort : {_id : 1} }
	] ) ;


//----------------------------------------------------------------------------------
//				For each date, get the number and the list of invoices, and
//               the number and the list of products
//----------------------------------------------------------------------------------

db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$invDate",
			invoice_list : { $addToSet : "$_id" } ,
			n_of_inv_lines : { $sum : 1},
			sold_products : { $addToSet : "$items.product.prodName" } }},
  { $addFields : { n_of_invoices : { $size: "$invoice_list"} } },
  { $addFields : { n_of_prods : { $size: "$sold_products"} } },
	{ $sort : {_id : 1} }
	] ) ;



//----------------------------------------------------------------------------------
// 						Get average invoice value (amount)
//----------------------------------------------------------------------------------

db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $group : { _id: null, avg_invoice_amount : { $avg : "$invoice_amount"	} } } 	] )  ;


//----------------------------------------------------------------------------------
// 				Get average invoice amount for each day 
//----------------------------------------------------------------------------------

db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},  invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $group : { _id: "$_id.invDate", avg_invoice_amount : { $avg : "$invoice_amount"	} } } ]	 )  ;




//==================================================================================
//                       II. Implementing subqueries-like solutions
//==================================================================================


//----------------------------------------------------------------------------------
// 		  Display all invoices issued to the same customer as for invoice 1111
//----------------------------------------------------------------------------------
// the subquery result is scalar (it contains a single value of `invoices.custID`)

// we will use a `findOne` command as a subquery
db.getCollection("invoices").aggregate([
		{$match : { custID : db.invoices.findOne({ _id :1111}).custID }}  // this extracts the customerid for invoice 1111
]);


//----------------------------------------------------------------------------------
// 	Display all invoices issued to customers located at the same postal code
//      as the zip code of the customer for invoice 1111
//----------------------------------------------------------------------------------
db.getCollection("invoices").aggregate([
		{$match : { custID : { $in :

    		db.customers.find({ postCode : 						// here we extract all the customers located
																//   at the same zipcode as the customer for invoice 1111
						db.customers.findOne(	{ _id: 					// here we extract the zipcode of
				                                                        //       the customer for invoice 1111
						    db.invoices.findOne({ _id :1111}).custID         // this line extracts the `custID` for invoice 1111

				    }).postCode

		    }, {_id: 1}).map( function(x) { return x._id; } )   // `map` will return customer ids as an array of values

		}}}
]);



//----------------------------------------------------------------------------------
// 					Display invoices issued in the first sales date
//----------------------------------------------------------------------------------
// the subquery result is scalar (it contains `min(invDate)`)


//----------------------------------------------------------------------------------
// sol.1: extract a scalar subquery result with `find` combined with `sort`, `limit` and `toArray`
db.invoices.aggregate([
	{ $match :{ invDate :
			(db.invoices.find().sort({ invDate : 1 }).limit(1).toArray())[0].invDate  // this is the scalar subquery
		} }
]);


//----------------------------------------------------------------------------------
// sol.2: extract a non-scalar subquery result with `find` combined with `sort`, `limit` and `map` (and `$in`)
db.invoices.aggregate([
	{ $match :{ invDate : { $in :
			db.invoices.find().sort({ invDate : 1 }).limit(1).map( function(x) { return x.invDate; } )   // this is the non-scalar subquery
		} } }
]);


//----------------------------------------------------------------------------------
// sol. 3: save the intermediary (subquery) results as a separate collection, then use it for filtering

// 3.1 this is the subquery result
db.invoices.aggregate([
	{ $group : { _id : null, invDate : { $min : "$invDate" }}},
  { $out : "first_day"}
]) ;

// 3.2a now, use it for filtering the 'invoice' collection
db.invoices.aggregate( [
		{$match : {invDate:   db.first_day.findOne().invDate  }}
	] );


// 3.2b another solution - with `lookup`
db.invoices.aggregate( [
    { $lookup: {
          from: "first_day",
          localField: "invDate",
          foreignField: "invDate",
          as: "first_date_invoices" } },
  	{ $addFields : { array_size : { $size: "$first_date_invoices"} } },
		{ $match : { array_size : { $gt : 0} } }
	] );


// 3.2c yet another solution - with `lookup`
db.first_day.aggregate( [
    { $lookup: {
          from: "invoices",
          localField: "invDate",
          foreignField: "invDate",
          as: "first_date_invoices" } },
		{ $unwind : "$first_date_invoices"}
	] );




//----------------------------------------------------------------------------------
// 						Display invoices issued in the first 7 days of sales
//----------------------------------------------------------------------------------
//

// bad news: next solution does not work...
db.invoices.aggregate([
    { $addFields : { min_date :  db.invoices.find().sort({ invDate : 1 }).limit(1).map( function(x) { return x.invDate; } ) [0]  } },
    { $addFields : { end_first_week : { $add: [ "$min_date", 7*24*60*60000 ]  } } } ,
    { $match : { invDate : { $lte : "$end_first_week"} } }    // this type of expression is not currently supported in MongoDB
    ]);

// good news: next solution does work (with `$expr` included in "$match")
db.invoices.aggregate([
    { $addFields : { min_date :  db.invoices.find().sort({ invDate : 1 }).limit(1).map( function(x) { return x.invDate; } ) [0]  } },
    { $addFields : { end_first_week : { $add: [ "$min_date", 7*24*60*60000 ]  } } } ,
    { $match: {  $expr : { $lte : [ "$invDate", "$end_first_week"  ]  } }}
    ]);




//==================================================================================
//                    III. Aggregations, comparisons, computed attributes...
//==================================================================================


//----------------------------------------------------------------------------------
//   Get, for each day of sales, the invoices with highest and the lowest amount
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// sol. 1
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},
			invoice_amount : { $sum : { $add : [
					{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
					{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : { invoice_amount : 1 } },
	{ $group : { _id: "$_id.invDate",
				biggestInvoice : { $last : "$_id._id" },
				biggestValue : { $last : "$invoice_amount" },
				smallestInvoice : { $first : "$_id._id" },
				smallestValue : { $first : "$invoice_amount" }
	} } ]) ;


//----------------------------------------------------------------------------------
// sol. 2 - using "project"
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},
			invoice_amount : { $sum : { $add : [
					{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
					{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : { invoice_amount : 1 } },
	{ $group : { _id: "$_id.invDate",
			biggestInvoice : { $last : "$_id._id" },
			biggestValue : { $last : "$invoice_amount" },
			smallestInvoice : { $first : "$_id._id" },
			smallestValue : { $first : "$invoice_amount" } } },
	{ $project : {
			_id : 1,
			invDate : "$_id.invDate",
			biggestInvoice : { number : "$biggestInvoice", amount : "$biggestValue"  },
			smallestInvoice : { number : "$smallestInvoice", amount : "$smallestValue"  }   }	} ]) ;



//----------------------------------------------------------------------------------
// 			Get the invoice list, with two properties, invoice number and date,
//         			and a "computed" one - month
//----------------------------------------------------------------------------------
db.invoices.aggregate ( [
	{ $project : { month : { $month : "$invDate" }, invNo : "$_id", invDate : "$invDate", _id : 0 } },
	{ $sort : { month : 1, invNo : 1 } }  ] ) ;



//----------------------------------------------------------------------------------
// 	Get the yearly number of invoices (well, the aren't too many years, but this is it)
//----------------------------------------------------------------------------------
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" } } } ,
	{ $group : { _id : { year_ : "$year_" }, n_of_invoices : { $sum : 1 } } },
	{ $sort : { year_ : 1 } }  ] ) ;



//----------------------------------------------------------------------------------
// 					Get the number of invoices for each pair (year, month)
//----------------------------------------------------------------------------------
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" }, month : { $month : "$invDate" } } } ,
	{ $group : { _id : { year_ : "$year_", month : "$month" }, n_of_invoices : { $sum : 1 } } },
	{ $sort : { year_ : 1, month : 1 } }  ] ) ;


//----------------------------------------------------------------------------------
// 					Get number of invoices for each combination (year, month, day)
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" }, month_ : { $month : "$invDate" },
		day_ : { $dayOfMonth : "$invDate" }  } } ,
	{ $group : { _id : { year_ : "$year_", month_ : "$month_", day_ : "$day_" },
				n_of_invoices : { $sum : 1 } } },
	{ $sort : { year_ : 1, month_ : 1, day_ : 1 } }  ] ) ;
// we need to sort the result

//----------------------------------------------------------------------------------
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" }, month_ : { $month : "$invDate" },
		day_ : { $dayOfMonth : "$invDate" }  } } ,
	{ $group : { _id : { year_ : "$year_", month_ : "$month_", day_ : "$day_" },
		n_of_invoices : { $sum : 1 } } },
	{ $sort : { "_id.year_" : 1, "_id.month_" : 1, "_id.day_" : 1 } }  ] ) ;
// now it is ok


//----------------------------------------------------------------------------------
// 					        Get the most frequently sold three products
//----------------------------------------------------------------------------------
db.invoices.aggregate ( [
	{ $unwind : "$items"  },
	{ $group : { _id : "$items.product.prodName", n_of_occurences : { $sum : 1 } }},
	{ $sort : { n_of_occurences : -1 } },
	{ $limit : 3 } ] ) ;


//----------------------------------------------------------------------------------
// 			Get, for each invoice, three amounts: without VAT, VAT, amount with VAT
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// sol. 1
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id"},
			amountWithoutVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice"] }  } ,
			amountVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice",
					"$items.product.percVAT" ] }  } ,
			amountWithVAT : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] } ,
				{ $multiply : ["$items.quantity", "$items.unitPrice",
				"$items.product.percVAT" ] } ] }}
	}},
	{ $sort : { _id : 1 } },
	{ $project : { _id : 1,  amounts : { withoutVAT : "$amountWithoutVAT",
		VAT : "$amountVAT", withVAT : "$amountWithVAT"  } } }
	]) ;

//----------------------------------------------------------------------------------
// sol. 2
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id"},
			amountWithoutVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice"] }  } ,
			amountVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice",
				"$items.product.percVAT" ] }  }
		}},
	{ $sort : { _id : 1 } },
	{ $project : { _id : 1,  amounts : { withoutVAT : "$amountWithoutVAT",
		VAT : "$amountVAT",
		withVAT : { $add : ["$amountWithoutVAT", "$amountVAT"]  } } } }
	]) ;

//----------------------------------------------------------------------------------
// sol. 3
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id"},
		amountWithoutVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice"] }  } ,
		amountVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }  }
		}},
	{ $sort : { _id : 1 } },
	{ $project : { _id : 0, invoice : { invoice_no : "$_id" },
		amounts : { withoutVAT : "$amountWithoutVAT", VAT : "$amountVAT",
			withVAT : { $add : ["$amountWithoutVAT", "$amountVAT"]  } } } }
		]) ;


//----------------------------------------------------------------------------------
// 						Get, for each invoice in September 2012, three amounts:
//									without VAT, VAT, amount with VAT
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// sol. 1
db.invoices.aggregate([
	{ $match : {  invDate : { $gte : new ISODate("2019-09-01T00:00:00Z"),
		$lte : new ISODate("2019-10-01T00:00:00Z") }}},
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", 	invDate : "$invDate"  }  ,
		amountWithoutVAT : { $sum : { $multiply : ["$items.quantity",
			"$items.unitPrice"] }  } ,
		amountVAT : { $sum : { $multiply : ["$items.quantity",
			"$items.unitPrice", "$items.product.percVAT" ] }  }
		}},
	{ $sort : { _id : 1 } },
	{ $project : { _id : 0, invoice : { invoice_no : "$_id._id",
		invDate : "$_id.invDate" },
		amounts : { withoutVAT : "$amountWithoutVAT", VAT : "$amountVAT",
			withVAT : { $add : ["$amountWithoutVAT", "$amountVAT"]  } } } }
	]) ;


//----------------------------------------------------------------------------------
// sol. 2
db.invoices.aggregate([
	{ $match : {  invDate : { $gte : new ISODate("2019-09-01T00:00:00Z"),
		$lte : new ISODate("2019-10-01T00:00:00Z") }}},
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", 	year_ : { "$year" : "$invDate" },
		month_ : { "$month" : "$invDate" }, day_ : { "$dayOfMonth" : "$invDate" }  } ,
		amountWithoutVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice"] }  } ,
		amountVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice",
		"$items.product.percVAT" ] }  }
		}},
	{ $sort : { _id : 1 } },
	{ $project : { _id : 0, invoice : { invoice_no : "$_id._id",
		year_ : "$_id.year_", month : "$_id.month_", zi : "$_id.day_" } ,
		amounts : { withoutVAT : "$amountWithoutVAT", VAT : "$amountVAT",
			withVAT : { $add : ["$amountWithoutVAT", "$amountVAT"]  } } } }
	]) ;

//----------------------------------------------------------------------------------
//  the next solution should be slower, because the filter is applies AFTER grouping
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", 	year_ : { "$year" : "$invDate" },
		month_ : { "$month" : "$invDate" }, day_ : { "$dayOfMonth" : "$invDate" }  } ,
		amountWithoutVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice"] }  } ,
		amountVAT : { $sum : { $multiply : ["$items.quantity",
			"$items.unitPrice", "$items.product.percVAT" ] }  }
		}},
	{ $sort : { _id : 1 } },
	{ $project : { _id : 0, invoice : { invoice_no : "$_id._id", year_ : "$_id.year_",
		month_ : "$_id.month_", zi : "$_id.day_" } ,
		amounts : { withoutVAT : "$amountWithoutVAT", VAT : "$amountVAT",
			withVAT : { $add : ["$amountWithoutVAT", "$amountVAT"]  } } } },
	{ $match : { "invoice.month_" : { $gte : 9, $lte : 9 } }  } 	]) ;



//----------------------------------------------------------------------------------
// 						Get the amount received (paid by the client) for each invoice
//----------------------------------------------------------------------------------
db.receipts.aggregate  ( [
	{ $unwind : "$invoicesCollected" },
	{ $group : { _id : "$invoicesCollected.invNo", paid : { $sum : "$invoicesCollected.amount"}}},
	{ $sort : { _id : 1 } }
	]) ;


//----------------------------------------------------------------------------------
//      Which is the region with the highest number of counties ?
//----------------------------------------------------------------------------------

// to be written during lecture
//...



//==================================================================================
//                     IV. Other queries involving two or more collections
//==================================================================================


//----------------------------------------------------------------------------------
//         Getting the total amount and the paid amount for each invoice
//----------------------------------------------------------------------------------

//----------------------------------------------------------------------------------
// we'll create a collection as a result of a query (aggregation) - sort of CREATE TABLE tab AS SELECT...)
db.inv.remove({}) ;
db.inv.insert ( db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", amountWithVAT : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : {_id : 1} }]
	).toArray() );

db.inv.createIndex({_id : 1}) ;

db.invoicesCollected.remove({});
db.invoicesCollected.insert ( db.receipts.aggregate  ([
	{ $unwind : "$invoicesCollected" },
	{ $group : { _id : "$invoicesCollected.invNo", amountPaid : { $sum : "$invoicesCollected.amount"}}},
	{ $sort : { _id : 1 } }
]).toArray() );

var cursor = db.invoicesCollected.find()	;
cursor.forEach(function(x) {
//	print (x ) ;
	db.inv.update( {"_id" : x._id }, {"$set" : {"amountPaid" : x.amountPaid  } } )  ;
	} ) ;
// It works !!!

db.inv.find().pretty() ;


//----------------------------------------------------------------------------------
// 						a multi-step solution based on join
//
// agggregate payments into a new collection
db.receipts.aggregate  ( [
	{ $unwind : "$invoicesCollected" },
	{ $group : { _id : "$invoicesCollected.invNo", amountPaid : { $sum : "$invoicesCollected.amount"}}},
	{ $sort : { _id : 1 } },
	{ $out: "paid_invoices"}
	]);

// join this with aggregated invoices
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", amountWithVAT : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : {_id : 1} },
    { $lookup: {
          from: "paid_invoices",
          localField: "_id",
          foreignField: "_id",
          as: "inv" } },
    { $project : { invoiceAmount: "$amountWithVAT", amountPaid : { $sum: "$inv.amountPaid"} } },
    { $project : { invoiceAmount: 1, amountPaid : 1, toBePaid : { $subtract : ["$invoiceAmount", "$amountPaid"] } } }
	] );



//----------------------------------------------------------------------------------
//          Which is the invoice with the greatest amount to be received ?
//----------------------------------------------------------------------------------
db.inv.aggregate([
	{ $group : { _id :  "_id" ,
		amountWithVAT : { $sum : "$amountWithVAT" },
		amountPaid : { $sum : "$amountPaid" },
		toBeReceived : { $sum : { $subtract : ["$amountWithVAT", "$amountPaid"] }  } }},
	{ $sort : { toBeReceived : -1 } },
	{ $limit : 1 }
]) ;



//----------------------------------------------------------------------------------
//Extract customers with at least the number of invoices of customer "Client 5 SRL"
//----------------------------------------------------------------------------------

// the ObjectId of "Client 1 SRL"
(db.customers.findOne({custName : "Client 5 SRL"}))._id;

// the number of invoices issued for "Client 5 SRL"
db.invoices.find({custID : (db.customers.findOne({custName : "Client 5 SRL"}))._id }).count();


// now, the solution
db.invoices.aggregate([
	{ $group : { _id : "$custID", n_of_invoices : { $sum : 1 }}},
  { $lookup: {
          from: "customers",
          localField: "_id",
          foreignField: "_id",
          as: "customer" } },
	{ $unwind : "$customer"},
	{ $project : { "customer_name" : "$customer.custName", n_of_invoices : 1}},
  { $addFields : { n_of_invoices_cust5 :
			db.invoices.find({custID : (db.customers.findOne({custName : "Client 5 SRL"}))._id }).count()
	 } },
  { $match: {  $expr : { $gte : [ "$n_of_invoices", "$n_of_invoices_cust5"  ]  } }}

]);

//----------------------------------------------------------------------------------
///// 			solution 2 - with intermediate results

// save a collection only with the number of invoices of "Client 5 SRL" and a `foo` column (set on 1)
db.invoices.aggregate([
	 { $match : {custID : (db.customers.findOne({custName : "Client 5 SRL"}))._id }},
   { $group: { _id: null, n_of_invoices_cust5: { $sum: 1 } }},
	 { $project : {n_of_invoices_cust5 : 1, _id : 0}},
   { $addFields : { foo : 1} },
	 { $out : "n_of_invoices_cust5"}
]);

// save a collection with the number of invoices for each customer and a `foo` column (set on 1)
db.invoices.aggregate([
	{ $group : { _id : "$custID", n_of_invoices : { $sum : 1 }}},
   { $addFields : { foo : 1} },
	 { $out : "customers_n_of_invoices"}
]);

// join the newly created collections
db.customers_n_of_invoices.aggregate([
  { $lookup: {
          from: "n_of_invoices_cust5",
          localField: "foo",
          foreignField: "foo",
          as: "customer_5" } },
	{ $unwind : "$customer_5"},
  { $match: {  $expr : { $gte : [ "$n_of_invoices", "$customer_5.n_of_invoices_cust5"  ]  } }},
  { $lookup: {
          from: "customers",
          localField: "_id",
          foreignField: "_id",
          as: "customer" } }

]);




//----------------------------------------------------------------------------------
//  Extract customers with the sales amount greater than or equal to
//   the customer "Client 5 SRL"
//----------------------------------------------------------------------------------

// we adapt the second solution from the previous example


// save a collection only with the sales for "Client 5 SRL" and a `foo` column (set on 1)
db.invoices.aggregate([
		{ $match : {custID : (db.customers.findOne({custName : "Client 5 SRL"}))._id }},
		{ $unwind : "$items" },
		{ $addFields : { line_amount : { $multiply: ["$items.quantity", "$items.unitPrice" ] } } },
		{ $addFields : { line_vat : { $multiply: ["$line_amount", "$items.product.percVAT" ] } } },
		{ $addFields : { line_with_vat : { $add: ["$line_amount", "$line_vat" ] } } },
		{ $group : { _id : null, sales_cust5 : { $sum : "$line_with_vat"  } } },
		{ $project : {n_of_invoices_cust5 : 1, _id : 0, sales_cust5: 1}},
  	{ $addFields : { foo : 1} },
		{ $out : "sales_cust5"}
]);


// save a collection with the sales for each customer and a `foo` column (set on 1)
db.invoices.aggregate([
		{ $unwind : "$items" },
		{ $addFields : { line_amount : { $multiply: ["$items.quantity", "$items.unitPrice" ] } } },
		{ $addFields : { line_vat : { $multiply: ["$line_amount", "$items.product.percVAT" ] } } },
		{ $addFields : { line_with_vat : { $add: ["$line_amount", "$line_vat" ] } } },
		{ $group : { _id : "$custID", sales_cust : { $sum : "$line_with_vat"  } } },
  	{ $lookup: {
          from: "customers",
          localField: "_id",
          foreignField: "_id",
          as: "customer" } },
		{ $unwind : "$customer"},
  	{ $addFields : { foo : 1} },
		{ $project : { _id :0, customer_name : "$customer.custName", sales_cust : 1, foo: 1} },
		{ $out : "sales_customers"}
]);



// join the newly created collections
db.sales_customers.aggregate([
  { $lookup: {
          from: "sales_cust5",
          localField: "foo",
          foreignField: "foo",
          as: "customer_5" } },
	{ $unwind : "$customer_5"},
  { $match: {  $expr : { $gte : [ "$sales_cust", "$customer_5.sales_cust5"  ]  } }}
]);

