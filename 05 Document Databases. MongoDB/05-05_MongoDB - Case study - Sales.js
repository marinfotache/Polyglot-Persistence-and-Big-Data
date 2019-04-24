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


//==================================================================================
//                              first collection - "counties"
db.counties.remove({}) ;
db.counties.save ( { _id : 'IS', countyName : 'Iasi', countyRegion  : 'Moldova' });
db.counties.insert ( { _id : 'B', countyName : 'Bucuresti'});
db.counties.save ( { _id : 'VN', countyName : 'Vrancea', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'NT', countyName : 'Neamt', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'SV', countyName : 'Suceava', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'VS', countyName : 'Vaslui', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'TM', countyName : 'Timis', countyRegion  : 'Banat'});
db.counties.save ( { _id : 'DJ', countyName : 'Dolj', countyRegion  : 'Oltenia'});

// indexes
db.counties.createIndex({_id : 1}, {unique: true}) ;
db.counties.createIndex({countyName: 1}, {unique: true}) ;
db.counties.createIndex({countyRegion : 1}, {unique: false}) ;

db.counties.find() ;

//==================================================================================
//                      second collection - "postalCodes"
db.postalCodes.remove({}) ;
db.postalCodes.save ( { _id : '700505', cityName : 'Iasi', countyCode : 'IS' });
db.postalCodes.save ( { _id : '701150', cityName : 'Pascani', countyCode : 'IS' });
db.postalCodes.save ( { _id : '706500', cityName : 'Vaslui', countyCode : 'VS' });
db.postalCodes.save ( { _id : '705300', cityName : 'Focsani', countyCode : 'VN' });
db.postalCodes.save ( { _id : '706400', cityName : 'Birlad', countyCode : 'VS' });
db.postalCodes.save ( { _id : '705800', cityName : 'Suceava', countyCode : 'SV' });
db.postalCodes.save ( { _id : '705550', cityName : 'Roman', countyCode : 'NT' });
db.postalCodes.save ( { _id : '701900', cityName : 'Timisoara', countyCode : 'TM' });
// indexes
db.postalCodes.createIndex({_id : 1}, {unique: true}) ;
db.postalCodes.createIndex({cityName: 1}, {unique: false}) ;
db.postalCodes.createIndex({countyCode: 1}, {unique: false}) ;

db.postalCodes.find() ;


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



//==================================================================================

//          third collection - customers
//   each document will be identified by system object id

db.customers.remove({}) ;

db.customers.save ( { custName : 'Client 1 SRL', custFiscalCode: 'R1001', 
	address : 'Tranzitiei, 13 bis',  postCode : '700505',
	contacts : [ { person : { 
			persCode : 'CNP1', familyName : 'Ioan', surName : 'Vasile', 
			address : 'I.L.Caragiale, 22', sex : 'B', postCode : '700505', 
			homePhone : '0232123456', officePhone : '0232987654', mobilePhone : '0742222222'
					}, position :  'Director general' }
		] }) ;

db.customers.save ( { custName : 'Client 2 SA', custFiscalCode: 'R1002', 
	postCode : '700505', phone : '0232212121',
	contacts : [ 
		{ person : { persCode : 'CNP2', familyName : 'Vasile', surName : 'Ion', 
			sex : 'B', postCode : '700505', homePhone : '0234234567', 
			officePhone : '0234876543', mobilePhone : '0794222223', email : 'Ion@a.ro'
					}, position :  'Director general' },
		{ person : { persCode : 'CNP3', familyName : 'Popovici', surName : 'Ioana', 
			address : 'V.Micle, Bl.I, Sc.B,Ap.2', sex : 'F', postCode : '701150', 
			homePhone : '0233534568', mobilePhone : '0744222224'
					}, position :  'Sef aprovizionare' }
		] }) ;
	
db.customers.save ( { custName : 'Client 3 SRL', custFiscalCode: 'R1003', address : 'Prosperitatii, 22', 
	postCode : '706500', phone : '0235222222',
	contacts : [ 
		{ person : { persCode : 'CNP4', familyName : 'Lazar', surName : 'Caraion', 
			address : 'M.Eminescu, 42', sex : 'B', postCode : '706500', homePhone : '0233534568', 
			officePhone : '0235222225', 
					}, position :  'Director general' },
		{ person : { persCode : 'CNP5', familyName : 'Iurea', surName : 'Simion', 
			address : 'I.Creanga, 44 bis', sex : 'B', postCode : '706500', 
			homePhone : '023567890', mobilePhone : '0235543210'
					}, position :  'Director financiar' }
		] }) ;

db.customers.save ( { custName : 'Client 4', address : 'Sapientei, 56', 
	contacts : [ 	{ person : { persCode : 'CNP6', familyName : 'Vasc', surName : 'Simona', 
			address : 'M.Eminescu, 13', sex : 'F', postCode : '701150', 
			officePhone : '0237432109', mobilePhone : '0794222227'
					}, position :  'Director general' } ] }) ;

db.customers.save ( { custName : 'Client 5 SRL', custFiscalCode: 'R1005',
	postCode : '701900', phone :  '0567111111',
	contacts : [ 	{ person : { persCode : 'CNP7', familyName : 'Popa', surName : 'Ioanid', 
			address : 'I.Ion, Bl.H2, Sc.C, Ap.45', sex : 'B', postCode : '701900', 
			homesPhone : '0238789012', officePhone : '0238321098'
					}, position :  'Sef aprovizionare' } ] }) ;

db.customers.save ( { custName : 'Client 6 SA', custFiscalCode: 'R1006', address : 'Pacientei, 33', 
	postCode : '705550',
	contacts : [ 	{ person : { persCode : 'CNP8', familyName : 'Bogacs', surName : 'Ildiko', 
			address : 'I.V.Viteazu, 67', sex : 'F', postCode : '705550', 
			homePhone : '0239890123', officePhone : '0239210987', mobilePhone : '0722222299'
					}, position :  'Director financiar' } ] }) ;

db.customers.save ( { custName : 'Client 7 SRL', custFiscalCode: 'R1007', address :  'Victoria Capitalismului, 2', 
	postCode : '701900', phone : '0567121212',
	contacts : [ 	{ person : { persCode : 'CNP9', familyName : 'Ioan', surName : 'Vasilica', 
			address : 'Garii, Bl.B4, Sc.A, Ap.1', sex : 'F', postCode : '701900', 
			homePhone : '0240901234', officePhone : '0240109876', mobilePhone : '0779422223'
					}, position :  'Sef aprovizionare' } ] }) ;

db.customers.update ( {custName : "Client 4"},  {$addToSet : { contacts : { $each : [ 
		{ person : 
			{ persCode : 'CNP7', familyName : 'Popa', surName : 'Ioanid', 
			address : 'I.Ion, Bl.H2, Sc.C, Ap.45', sex : 'B', postCode : '701900', 
			homesPhone : '0238789012', officePhone : '0238321098'}, 
			  position :  'Consultant aprovizionare' },
		{ person : 
			{ persCode : 'CNP8', familyName : 'Bogacs', surName : 'Ildiko', 
			address : 'I.V.Viteazu, 67', sex : 'F', postCode : '705550', 
			homePhone : '0239890123', officePhone : '0239210987', mobilePhone : '0722222299'}, 
			  position :  'Consultant financiar' } ] }}}) ;					
		

db.customers.find().pretty() ;


//==================================================================================

//         		fourth collection - products
// 	example of upsert  (update combined with insert)
//db.products.remove({}) ;

db.products.update ( {_id : 1}, 
	{_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Cosmetice',  percVAT  : .24 },
		{upsert : true} ) ;
db.products.update ( {_id : 2}, 
	{_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		{upsert : true} ) ;	
db.products.update ( {_id : 3}, 
	{_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		{upsert : true} ) ;	
db.products.update ( {_id : 4}, 
	{_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		{upsert : true} ) ;
db.products.update ( {_id : 5}, 
	{_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Cosmetice',  percVAT  : .24 },
		{upsert : true} ) ;
db.products.update ( {_id : 6}, 
	{_id: 6, prodName : 'Produs 6', mu : 'p250g', prodCateg : 'Cafea',  percVAT  : .24 },
			{upsert : true} ) ;


db.products.find().pretty() ;


//==================================================================================

//
//		fifth collection: invoices
//
db.invoices.remove({}) ;

// invoice 1111
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;

db.invoices.insert ( {_id : 1111, invDate : new ISODate("2012-08-01T11:00:00Z"), 
	custID :  myClient._id,
	items : [
		{ line : 1, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', 
		  	prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 50, 
		  unitPrice : 1000 },
		{ line : 2, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', 
		  	prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 75, 
		  unitPrice : 1050 },
		{ line : 3, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', 
		  	prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 500, 
		  unitPrice : 7060 }  ] }) ;


// invoice 1112
var myClient = db.customers.find ( { custName : 'Client 5 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1112, invDate : new ISODate("2012-08-01T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 80, 
		  unitPrice : 1030 },
		{ line : 2, 
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40, 
		  unitPrice : 750 }  ], 
		  notes : 'Probleme cu transportul' }) ;


// invoice 1113
var myClient = db.customers.find ( { custName : 'Client 2 SA'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1113, invDate : new ISODate("2012-08-01T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 100, 
		  unitPrice : 975 } ] }) ;


// invoice 1114
var myClient = db.customers.find ( { custName : 'Client 6 SA'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1114, invDate : new ISODate("2012-08-01T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 70, 
		  unitPrice : 1070 },
		{ line : 2, 
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 30, 
		  unitPrice : 1705 },
		{ line : 3, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 700, 
		  unitPrice : 7064 }  ] }) ;


// invoice 1115
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1115, invDate : new ISODate("2012-08-02T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 150, 
		  unitPrice : 925 } ] } ) ;


// invoice 1116
var myClient = db.customers.find ( { custName : 'Client 7 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1116, invDate : new ISODate("2012-08-02T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 125, 
		  unitPrice : 930 } ],
	notes : 'Pretul propus initial a fost modificat' }) ;
 
		  
// invoice 1117
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1117, invDate : new ISODate("2012-08-03T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 100, 
		  unitPrice : 1000 },
		{ line : 2, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 100, 
		  unitPrice : 950 }  ] } ) ;		  	  


// invoice 1118
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1118, invDate : new ISODate("2012-08-04T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 30, 
		  unitPrice : 1100 },
		{ line : 2, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 150, 
		  unitPrice : 930 }  ] }) ;	


// invoice 1119
var myClient = db.customers.find ( { custName : 'Client 3 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1119, invDate : new ISODate("2012-08-07T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 35, 
		  unitPrice : 1090 },
		{ line : 2, 
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40, 
		  unitPrice : 700 },
		{ line : 3, 
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 50, 
		  unitPrice : 1410 },
		{ line : 4, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 750, 
		  unitPrice : 6300 }  ] }) ;


// invoice 1120
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1120, invDate : new ISODate("2012-08-07T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 80, 
		  unitPrice : 1120 } ] }) ;


// invoice 1121
var myClient = db.customers.find ( { custName : 'Client 4'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1121, invDate : new ISODate("2012-08-07T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 550, 
		  unitPrice : 7064 },
		{ line : 2, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 100, 
		  unitPrice : 1050 }  ] }) ;	 


// invoice 1122  - empty (no lines)
var myClient = db.customers.find ( { custName : 'Client 5 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 1122, invDate : new ISODate("2012-08-07T11:00:00Z"), custID : mycustomerId }) ;	 


// invoice 2111
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2111, invDate : new ISODate("2012-08-14T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 57, 
		  unitPrice : 1000 },
		{ line : 2, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 79, 
		  unitPrice : 1050 },
		{ line : 3, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 510, 
		  unitPrice : 7060 }  ] }) ;


// invoice 2112
var myClient = db.customers.find ( { custName : 'Client 5 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2112, invDate : new ISODate("2012-08-14T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 85, 
		  unitPrice : 1030 },
		{ line : 2, 
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 65, 
		  unitPrice : 750 }  ], 
		  notes : 'Probleme cu transportul' }) ;


// invoice 2113
var myClient = db.customers.find ( { custName : 'Client 2 SA'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2113, invDate : new ISODate("2012-08-14T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 120, 
		  unitPrice : 975 } ] }) ;
		  
		  
// invoice 2115
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2115, invDate : new ISODate("2012-08-15T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 110, 
		  unitPrice : 925 } ] } ) ;


// invoice 2116
var myClient = db.customers.find ( { custName : 'Client 7 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2116, invDate : new ISODate("2012-08-15T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 135, 
		  unitPrice : 930 } ],
	notes : 'Pretul propus initial a fost modificat' }) ;
 		  

// invoice 2117
var myClient = db.customers.find ( { custName : 'Client 1 SRL' }) ;
var mycustomerId ;
myClient.forEach(function(x) { 	mycustomerId = x._id ;  } ) ;
db.invoices.insert ( {_id : 2117, invDate : new ISODate("2012-08-16T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 150, 
		  unitPrice : 1000 },
		{ line : 2, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 110, 
		  unitPrice : 950 }  ] }) ;		  	  


// invoice 2118
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) { mycustomerId = x._id ;  } ) ;
db.invoices.insert ( {_id : 2118, invDate : new ISODate("2012-08-16T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 39, 
		  unitPrice : 1100 },
		{ line : 2, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 120, 
		  unitPrice : 930 }  ] }) ;	


// invoice 2119
var myClient = db.customers.find ( { custName : 'Client 3 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {	mycustomerId = x._id ; } ) ;
db.invoices.insert ( {_id : 2119, invDate : new ISODate("2012-08-21T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 35, 
		  unitPrice : 1090 },
		{ line : 2, 
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40, 
		  unitPrice : 700 },
		{ line : 3, 
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 55, 
		  unitPrice : 1410 },
		{ line : 4, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 755, 
		  unitPrice : 6300 }  ] }) ;


// invoice 2121
var myClient = db.customers.find ( { custName : 'Client 4'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2121, invDate : new ISODate("2012-08-21T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 550, 
		  unitPrice : 7064 },
		{ line : 2, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 103, 
		  unitPrice : 1050 }  ] }) ;	 


// invoice 2122  - empty (no lines)
var myClient = db.customers.find ( { custName : 'Client 5 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 2122, invDate : new ISODate("2012-08-22T11:00:00Z"), custID : mycustomerId }) ;	 
		  

// invoice 3111
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {mycustomerId = x._id ; } ) ;
db.invoices.insert ( {_id : 3111, invDate : new ISODate("2012-09-01T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 57, 
		  unitPrice : 1000 },
		{ line : 2, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 79, 
		  unitPrice : 1050 },
		{ line : 3, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 510, 
		  unitPrice : 7060 }  ] }) ;


// invoice 3112
var myClient = db.customers.find ( { custName : 'Client 5 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 3112, invDate : new ISODate("2012-09-01T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 85, 
		  unitPrice : 1030 },
		{ line : 2, 
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 65, 
		  unitPrice : 750 }  ], 
		  notes : 'Probleme cu transportul' }) ;


// invoice 3113
var myClient = db.customers.find ( { custName : 'Client 2 SA'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 3113, invDate : new ISODate("2012-09-02T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 120, 
		  unitPrice : 975 } ] }) ;


// invoice 3115
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 3115, invDate : new ISODate("2012-09-02T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 110, 
		  unitPrice : 925 } ] } ) ;


// invoice 3116
var myClient = db.customers.find ( { custName : 'Client 7 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {
	mycustomerId = x._id ; 
	} ) ;
db.invoices.insert ( {_id : 3116, invDate : new ISODate("2012-09-10T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 135, 
		  unitPrice : 930 } ],
	notes : 'Pretul propus initial a fost modificat' }) ;
 		  

// invoice 3117
var myClient = db.customers.find ( { custName : 'Client 1 SRL' }) ;
var mycustomerId ;
myClient.forEach(function(x) { 	mycustomerId = x._id ;  } ) ;
db.invoices.insert ( {_id : 3117, invDate : new ISODate("2012-09-10T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 150, 
		  unitPrice : 1000 },
		{ line : 2, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 110, 
		  unitPrice : 950 }  ] }) ;		  	  


// invoice 3118
var myClient = db.customers.find ( { custName : 'Client 1 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) { mycustomerId = x._id ;  } ) ;
db.invoices.insert ( {_id : 3118, invDate : new ISODate("2012-09-17T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 39, 
		  unitPrice : 1100 },
		{ line : 2, 
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 120, 
		  unitPrice : 930 }  ] }) ;	


// invoice 3119
var myClient = db.customers.find ( { custName : 'Client 3 SRL'}) ;
var mycustomerId ;
myClient.forEach(function(x) {	mycustomerId = x._id ; } ) ;
db.invoices.insert ( {_id : 3119, invDate : new ISODate("2012-10-07T11:00:00Z"), custID : mycustomerId,
	items : [
		{ line : 1, 
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 35, 
		  unitPrice : 1090 },
		{ line : 2, 
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40, 
		  unitPrice : 700 },
		{ line : 3, 
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 55, 
		  unitPrice : 1410 },
		{ line : 4, 
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 755, 
		  unitPrice : 6300 }  ] }) ;

db.invoices.createIndex({_id : 1}, {unique: true}) ;
db.invoices.createIndex({invDate: 1}, {unique: false}) ;
db.invoices.createIndex({codcl: 1}, {unique: false}) ;

db.invoices.find().pretty() ;


//==================================================================================

//
//		the sixth collection: receipts
//
db.receipts.drop()

// receipt 1234
db.receipts.insert ( { _id : 1234, recDate : new ISODate("2012-08-12T11:00:00Z"), 
	docCode: 'OP', docNo : '121', docDate : new ISODate("2012-08-10T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1111, amount : 4527400 } ] } ) ;


// receipt 1235
db.receipts.insert ( { _id : 1235, recDate : new ISODate("2012-08-18T11:00:00Z"), 
	docCode: 'CEC', docNo : '212', docDate : new ISODate("2012-08-17T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1112, amount : 129488 },
		{ invNo : 2112, amount : 158506 }	] } ) ;


// receipt 1236
db.receipts.insert ( { _id : 1236, recDate : new ISODate("2012-09-07T11:00:00Z"), 
	docCode: 'OP', docNo : '321', docDate : new ISODate("2012-09-05T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1113, amount : 50000 },
		{ invNo : 2113, amount : 50000 },
		{ invNo : 3113, amount : 50000 }	] } ) ;


// receipt 1237
db.receipts.insert ( { _id : 1237, recDate : new ISODate("2012-09-08T11:00:00Z"), 
	docCode: 'CEC', docNo : '445', docDate : new ISODate("2012-09-06T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1114, amount : 6272728 } ] } ) ;


// receipt 1238
db.receipts.insert ( { _id : 1238, recDate : new ISODate("2012-09-08T11:00:00Z"), 
	docCode: 'OP', docNo : '532', docDate : new ISODate("2012-09-06T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1115, amount : 70000 },
		{ invNo : 1117, amount : 100000 },
		{ invNo : 1118, amount : 100000 },
		{ invNo : 1120, amount : 70000 }	] } ) ;


// receipt 1239
db.receipts.insert ( { _id : 1239, recDate : new ISODate("2012-09-09T11:00:00Z"), 
	docCode: 'OP', docNo : '622', docDate : new ISODate("2012-09-07T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1116, amount : 130200 }	] } ) ;


// receipt 1240
db.receipts.insert ( { _id : 1240, recDate : new ISODate("2012-09-09T11:00:00Z"), 
	docCode: 'OP', docNo : '432', docDate : new ISODate("2012-09-05T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1119, amount : 6015408 }	] } ) ;


// receipt 1241
db.receipts.insert ( { _id : 1241, recDate : new ISODate("2012-09-18T11:00:00Z"), 
	docCode: 'OP', docNo : '213', docDate : new ISODate("2012-09-11T11:00:00Z"), 
	invoicesCollected : [
		{ invNo : 1121, amount : 4935248 },
		{ invNo : 2121, amount : 4938776 }	] } ) ;


db.receipts.find().pretty() ;


/*  add a set on invoices, based on existing invoices:
    * changind invoice number (increment by 1000)
    * changing invoice dates (increment by 365)   
    * removing a line from two or three incoices            
*/

//==================================================================================
//
//                                      Queries on SALES        
//
//==================================================================================

// task during lectures: write, whenever appropriate, solutions using join


//==================================================================================
//                         Basic queries (data retrieval, aggregation)

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
db.invoices.aggregate( 
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $group : { _id: null, avg_invoice_amount : { $avg : "$invoice_amount"	} } } 	 )  ; 


// Average invoice amount for each day (with sales) 
db.invoices.aggregate( 
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id", invDate : "$invDate"},  invoice_amount : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $group : { _id: "$_id.invDate", avg_invoice_amount : { $avg : "$invoice_amount"	} } } 	 )  ; 




//==================================================================================
//                    Aggregations, comparisons, computed attributes...
        

//      Which is the region with the highest number of counties ?
        

// Get, for each day of sales, the invoices with highest and the lowest amount 
db.invoices.aggregate( 
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
		smallestValue : { $first : "$invoice_amount" } } } ) ;


// Get, for each day of sales, the invoices with highest and the lowest amount 		
// second solution - using "project"
db.invoices.aggregate( 
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
		smallestInvoice : { number : "$smallestInvoice", amount : "$smallestValue"  }   }	} ) ;


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
db.invoices.aggregate( 
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
		) ;
				
// get, for each invoice, three amounts: without VAT, VAT, amount with VAT - sol. 2
db.invoices.aggregate( 
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
		) ;
				
// get, for each invoice, three amounts: without VAT, VAT, amount with VAT - sol. 3
db.invoices.aggregate( 
	{ $unwind : "$items" },
	{ $group : { _id : { _id : "$_id"},  
		amountWithoutVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice"] }  } ,
		amountVAT : { $sum : { $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }  } 
		}},
	{ $sort : { _id : 1 } },	
	{ $project : { _id : 0, invoice : { invoice_no : "$_id" },  
		amounts : { withoutVAT : "$amountWithoutVAT", VAT : "$amountVAT", 
			withVAT : { $add : ["$amountWithoutVAT", "$amountVAT"]  } } } } 
		) ;

		
// get, for each invoice in September 2012, three amounts: without VAT, VAT, amount with VAT - sol. 1
db.invoices.aggregate( 
	{ $match : {  invDate : { $gte : new ISODate("2012-09-01T00:00:00Z"), 
		$lte : new ISODate("2012-10-01T00:00:00Z") }}},
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
	) ;


// get, for each invoice in September 2012, three amounts: without VAT, VAT, amount with VAT - sol. 2
db.invoices.aggregate( 
	{ $match : {  invDate : { $gte : new ISODate("2012-09-01T00:00:00Z"), 
		$lte : new ISODate("2012-10-01T00:00:00Z") }}},
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
	) ;

//  the next solution should be slower, because the filter is applies AFTER grouping  
db.invoices.aggregate( 
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
	{ $match : { "invoice.month_" : { $gte : 9, $lte : 9 } }  } 	) ;


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
db.inv.insert ( db.invoices.aggregate( 
	{ $unwind : "$items" },
	{ $group : { _id : "$_id", amountWithVAT : { $sum : { $add : [
				{ $multiply : ["$items.quantity", "$items.unitPrice" ] },
				{ $multiply : ["$items.quantity", "$items.unitPrice", "$items.product.percVAT" ] }
															] } } } } ,
	{ $sort : {_id : 1} }
	).toArray() );

db.inv.createIndex({_id : 1}) ;

db.invoicesCollected.remove({});
db.invoicesCollected.insert ( db.receipts.aggregate  ( 
	{ $unwind : "$invoicesCollected" },
	{ $group : { _id : "$invoicesCollected.invNo", amountPaid : { $sum : "$invoicesCollected.amount"}}},
	{ $sort : { _id : 1 } }
	).toArray() );

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
db.inv.aggregate( 
	{ $group : { _id :  "_id" ,  
		amountWithVAT : { $sum : "$amountWithVAT" },
		amountPaid : { $sum : "$amountPaid" },
		toBeReceived : { $sum : { $subtract : ["$amountWithVAT", "$amountPaid"] }  } }},
	{ $sort : { toBeReceived : -1 } },
	{ $limit : 1 } 	
		) ;
	

