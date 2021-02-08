//===============================================================================
//                                      Case study:  SALES
//===============================================================================

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
//                                      Queries on SALES
//
//==================================================================================

//==================================================================================
//                         Basic queries (data retrieval, aggregation)

//======== Queries
//      Some previously discussed solutions (script 04-03...)
//
//--    Display information about the county to which postal code '700505' belongs to


// one of the the simplest...
var row_codpost = db.postalCodes.findOne ({_id : '700505'}) ;
db.counties.find({'_id' : row_codpost['countyCode']}) ;

// another one based on a cursor & hasNext
var myCursor = db.postalCodes.find ({_id : '700505'}) ;
var myRow = myCursor.hasNext() ? myCursor.next() :null ;
if (myRow) {
	var myCountyCode = myRow.countyCode ;
	print (myCountyCode) ;
	}
db.counties.find({'_id' : myCountyCode }) ;

//  another one based on a cursor & forEach
var myCursor = db.postalCodes.find ({_id : '700505'}) ;
var  myCountyCode ;
myCursor.forEach(function(x) {
	myCountyCode = x.countyCode ;
	print(myCountyCode) ;
	} ) ;
db.counties.find({'_id' : myCountyCode }) ;


//--    A solution based on Aggregation Framework and operator "$out"
db.postalCodes.aggregate( [
	{ $match: { _id : "700505" }},
	{ $project : { countyCode :1, _id: 0} },
	{ $out: "region_zip"}
	] ) ;
// use the newly created collection	as a parameter for searching in "counties"
db.counties.find({'_id' :(db.region_zip.findOne()).countyCode }) ;



//--    A new solution based on Aggegation Framework and pseudo-join
// It is not the most recommended in this case, but we can get the idea

// clean up the collection containing the result
db.result.remove({}) ;

// aggregate returns a cursor which will be processed row by row
db.postalCodes.aggregate({ $match: {_id : "700505"}}).forEach(function(pc)
	{	var county = db.counties.findOne({_id: pc.countyCode});
        if (county !== null)
        {
        	pc.countyName = county.countyName;
            pc.countyRegion = county.countyRegion;
            } else
            {
            	pc.countyName = "not found";
                pc.countyRegion = "not found";
            }
            db.result.insert(pc)
        }   ) ;
// display collection results
db.result.find()


//--------------------------------------------------------------------------------
// New in MongoDB version 3.2: $lookup - which emulates a left outer join on two
//   collections in the same database
//--------------------------------------------------------------------------------

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
])

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
])


// display the county to which postal code '700505' belongs to
db.postalCodes.aggregate([
    { $lookup: {
          from: "counties",
          localField: "countyCode",
          foreignField: "_id",
          as: "post_codes__counties" } },
    { $match: { _id : "700505"   } },
    { $project: {post_codes__counties : 1} }
])



//----------------------------------------------------------------------
//--     Show the county name and the region for city of Pascani

// Store the results of "find" into year_ array variable ("myArray")
myArray = db.postalCodes.find ({_id : '701150'}).toArray() ;
// Use the array variable (only the first array element) in "find"
db.counties.find({'_id' : myArray[0].countyCode }) ;


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


//----------------------------------------------------------------------

// Problem: get all the postal codes for region of Moldova ?

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


// pseudo-join
db.result.remove({}) ;
db.counties.aggregate({ $match: {countyRegion  : 'Moldova'}}).forEach(function(pc)
	{	var postal_codes = db.postalCodes.find({countyCode: pc._id}).toArray() ;
        if (postal_codes.length > 0)
        {
        	pc.postalCodes = postal_codes;
            } else
            {
        	pc.postalCodes = "not found";
            }
            db.result.insert(pc)
        }   ) ;
// display collection results
db.result.find().pretty() ;


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



//--    Display number of counties for each region
db.counties.aggregate(
    { $group: { _id: {region: "$countyRegion"}, n_of_counties: { $sum: 1} } },
     { $sort: {_id: 1}} );


//--    Display all the counties of each region
db.counties.aggregate(
    { $group: { _id: {region: "$countyRegion"},
       n_of_counties: { $sum: 1},
       counties: { $addToSet: "$countyName"} } },
     { $sort: {_id: 1}} );


//-- get the overall number of invoices
db.invoices.aggregate(
	{ $group: { _id: null, n_of_invoices : { $sum : 1 } }
	}  ) ;


//-- number of daily invoices
db.invoices.aggregate  ([
	{ $group: { _id: "$invDate", n_of_invoices : { $sum : 1 } } }
	] ) ;


//-- number of daily invoices ordered by dates
db.invoices.aggregate  ([
	{ $group: { _id: "$invDate", n_of_invoices : { $sum : 1 } } },
	{ $sort : {invDate : 1} }
	] ) ;


//-- invoice amount without VAT
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", amount_without_VAT :
		{ $sum : {$multiply : ["$items.quantity", "$items.unitPrice" ] } } } },
	{ $sort : {_id : 1} }
	] ) ;


// invoice amount with VAT
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] },
				] } } } } ,
	{ $sort : {_id : 1} }
	] ) ;




// daily sales
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$invDate", daily_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : {invDate : 1} }
	] ) ;


//	something different from traditional SQL: daily (sold) products
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$invDate", sold_products : { $addToSet : "$items.product.prodName" } }},
	{ $sort : {invDate : 1} }
	] ) ;


// Average invoice value (amount)
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $group : { _id: null, avg_invoice_amount : { $avg : "$invoice_amount"	} } } 	] )  ;


// Average invoice amount for each day (with sales)
db.invoices.aggregate( [
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},  invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $group : { _id: "$_id.invDate", avg_invoice_amount : { $avg : "$invoice_amount"	} } } ]	 )  ;




//==================================================================================
//                    Aggregations, comparisons, computed attributes...


//      Which is the region with the highest number of counties ?

// Get, for each day of sales, the invoices with highest and the lowest amount
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},  invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : { invoice_amount : 1 } },
	{ $group : { _id: "$_id.invDate",
		biggestInvoice : { $last : "$_id._id" },
		biggestValue : { $last : "$invoice_amount" },
		smallestInvoice : { $first : "$_id._id" },
		smallestValue : { $first : "$invoice_amount" } } } ]) ;


// Get, for each day of sales, the invoices with highest and the lowest amount
// second solution - using "project"
db.invoices.aggregate([
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},  invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : { invoice_amount : 1 } },
	{ $group : { _id: "$_id.invDate",
		biggestInvoice : { $last : "$_id._id" },
		biggestValue : { $last : "$invoice_amount" },
		smallestInvoice : { $first : "$_id._id" },
		smallestValue : { $first : "$invoice_amount" } } },
	{ $project : { _id : 1,
		invDate : "$_id.invDate",
		biggestInvoice : { number : "$biggestInvoice", amount : "$biggestValue"  },
		smallestInvoice : { number : "$smallestInvoice", amount : "$smallestValue"  }   }	} ]) ;


// invoice list, with two properties, invoice number and date, and a "computed" one - month
db.invoices.aggregate ( [
	{ $project : { month : { $month : "$invDate" }, invNo : "$_id", invDate : "$invDate", _id : 0 } },
	{ $sort : { month : 1, invNo : 1 } }  ] ) ;


// 	yearly number of invoices (well, the aren't too many years, but this is it)
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" } } } ,
	{ $group : { _id : { year_ : "$year_" }, n_of_invoices : { $sum : 1 } } },
	{ $sort : { year_ : 1 } }  ] ) ;


// 	number of invoices for each pair (year, month)
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" }, month : { $month : "$invDate" } } } ,
	{ $group : { _id : { year_ : "$year_", month : "$month" }, n_of_invoices : { $sum : 1 } } },
	{ $sort : { year_ : 1, month : 1 } }  ] ) ;


// 	number of invoices for each combination (year, month, day)
db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" }, month_ : { $month : "$invDate" },
		day_ : { $dayOfMonth : "$invDate" }  } } ,
	{ $group : { _id : { year_ : "$year_", month_ : "$month_", day_ : "$day_" },
		n_of_invoices : { $sum : 1 } } },
	{ $sort : { year_ : 1, month_ : 1, day_ : 1 } }  ] ) ;
// we need to sort the result

db.invoices.aggregate ( [
	{ $project : { year_ : { $year : "$invDate" }, month_ : { $month : "$invDate" },
		day_ : { $dayOfMonth : "$invDate" }  } } ,
	{ $group : { _id : { year_ : "$year_", month_ : "$month_", day_ : "$day_" },
		n_of_invoices : { $sum : 1 } } },
	{ $sort : { "_id.year_" : 1, "_id.month_" : 1, "_id.day_" : 1 } }  ] ) ;
// not it is ok


// the most frequently sold three products
db.invoices.aggregate ( [
	{ $unwind : "$items"  },
	{ $group : { _id : "$items.product.prodName", n_of_occurences : { $sum : 1 } }},
	{ $sort : { n_of_occurences : -1 } },
	{ $limit : 3 } ] ) ;


////
// get, for each invoice, three amounts: without VAT, VAT, amount with VAT - sol. 1
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

// get, for each invoice, three amounts: without VAT, VAT, amount with VAT - sol. 2
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

// get, for each invoice, three amounts: without VAT, VAT, amount with VAT - sol. 3
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


// get, for each invoice in September 2012, three amounts: without VAT, VAT, amount with VAT - sol. 1
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


// get, for each invoice in September 2012, three amounts: without VAT, VAT, amount with VAT - sol. 2
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


// get the amount received (paid by the client) for each invoice
db.receipts.aggregate  ( [
	{ $unwind : "$invoicesCollected" },
	{ $group : { _id : "$invoicesCollected.invNo", paid : { $sum : "$invoicesCollected.amount"}}},
	{ $sort : { _id : 1 } }
	]) ;




//==================================================================================
//                         Queries involving two or more collections

//-------------------------------------------------------------------------------
//  getting the total amount and the paid amount for each invoice
//
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


// solution based on join

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
	] )




//-----------------------------
// Which is the invoice with the greatest amount to be received (paid by the customer)
db.inv.aggregate([
	{ $group : { _id :  "_id" ,
		amountWithVAT : { $sum : "$amountWithVAT" },
		amountPaid : { $sum : "$amountPaid" },
		toBeReceived : { $sum : { $subtract : ["$amountWithVAT", "$amountPaid"] }  } }},
	{ $sort : { toBeReceived : -1 } },
	{ $limit : 1 }
]) ;

