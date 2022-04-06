//===============================================================================
//                           Queries and cursors in MongoDB
// last update: 2022-03-07


//===============================================================================
//--  some of the Examples are taken/inspired from the book
//--    "MongoDB in action" by Banker et al., Manning Publication, 2016
//--   other examples are taken/inspired from the MongoDB online documentation
//--     and most examples are completely uninspiring
//===============================================================================

//--    Set (if necessary) "sdbis2022" as current db
//use sdbis2022


//===============================================================================
//         Queries - recap from previous scripts and some new basic features
//===============================================================================


// Retrieve all the documents in collection "first_collection"
db.first_collection.find() ;

//===============================================================================
//                       Projection (selecting attributes)
//===============================================================================

// Display values of only atribute "title"
db.first_collection.find( {}, { "title" : 1}) ;

// Display values of all attributes except "tags"
db.first_collection.find( {}, { "tags" : 0}) ;

// Display values of all attributes except "tags" and "comments"
db.first_collection.find( {}, { "tags" : 0, "comments" : 0}) ;

// Display values for only "title" and "votes" (and the default ObjectId)
db.first_collection.find( {}, { "title" : 1, "votes" : 1 } ) ;

// Display values for only "title" and "votes" but without ObjectId
db.first_collection.find( {}, { "title" : 1, "votes" : 1, "_id" :0 } ) ;


//===============================================================================
//                         Basic selection/filtering
//===============================================================================


// Display first three documents in the collection "first_collection" (LIMIT clause)
db.first_collection.find( {}, { "title" : 1, "_id" : 0 }, 3 ) ;

// Display title (without ObjectId) for the second and the third document (LIMIT & SKIP)
db.first_collection.find( {}, { "title" : 1, "_id" : 0 }, 2,1  ) ;

// Retrieve documents for which the value of attribute "title" is "NoSQL Databases"
db.first_collection.find( {"title" : "NoSQL Databases" }) ;

// Retrieve documents with tag "NoSQL"
db.first_collection.find( {"tags" : "NoSQL" }) ;

// Retrieve documents (blog entries) with comments written by user "dragos"
db.first_collection.find( {"comments.user" : "dragos" }) ;

// Retrieve documents (blog entries) with the FIRST comment written by user "dragos"
db.first_collection.find( {"comments.0.user" : "dragos" }) ;

// Retrieve documents for which the author is declared (specified)
db.first_collection.find( { author : {$exists : 1 } }) ;

// Retrieve blog entries which have received at least two votes
// ... but, before this, we increment by 5 all the blog entries tagged with "NoSQL"
db.first_collection.update ( {"tags" : "NoSQL" }, {$inc : {"votes" : 5}}, false, true ) ;
// ... and here is the query
db.first_collection.find( {"votes" : {"$gte" : 2 } }) ;

// Retrieve blog entries which have received a number of votes which is different than 5
db.first_collection.find( {"votes" : {"$ne" : 5 } }) ;

// Retrieve blog entries with 0 (zero) or 5 votes
db.first_collection.find( {"votes" : {"$in" : [5, 0] } } ) ;

// Retrieve blog entries commented by "bjones" or the commenter is unknown
db.first_collection.find( {"comments.user" : { "$in" : ["bjones", "unknown" ]} } ) ;


//===============================================================================
//                  (Logical) Operator AND
//===============================================================================

// Retrieve blog entries with the number of votes between 2 and 5
db.first_collection.find( {"votes" : {"$gte" : 2, "$lte" : 5 } }) ;

// Retrieve blog entries tagged "NoSQL" AND at least a comment posted by "dragos"
db.first_collection.find( {"tags" : "NoSQL" , "comments.user" : "dragos"} ) ;

// solution with `$and`
db.first_collection.find( { "$and" : [ {"tags" : "NoSQL"}, {"comments.user" : "dragos" } ] } )  ;



//===============================================================================
//                     (Logical) Operator "$or"
//===============================================================================

// Retrieve blog entries tagged "NoSQL" OR at least a comment posted by "dragos"
db.first_collection.find( { "$or" : [ {"tags" : "NoSQL"}, {"comments.user" : "dragos" } ] } )  ;


//===============================================================================
//            Operators "$ne", "$not", "$nin"
//===============================================================================

//--      $ne selects the documents where the value of the field is not equal (i.e. !=)
// to the specified value. This includes documents that do not contain the field.

// Retrieve all the blog entries excepted those written by "Valerica Greavu-Serban"
db.first_collection.find( { author: { $ne : "Valerica Greavu-Serban" } } ) ;

// Retrieve the blog entries for which the author is declared (specified) and the
// author is not "Valerica Greavu-Serban"
db.first_collection.find( { author : {$exists : 1, $ne : "Valerica Greavu-Serban"  },  }) ;

// Retrieve all the blog entries which are not commented by user "dragos"
db.first_collection.find( {"comments.user": { $ne:  "dragos"} } ) ;


//-- the "$not" operator only affects other operators and cannot check
//  fields and documents independently.
// So, operator "$not" is used for logical disjunctions and the "$ne" operator
//    can test the contents of fields directly.

// Retrieve blog entries which have not received between 3 and 6 votes
//   (including blog entries without attribute "votes")
db.first_collection.find( { votes: {$not: { $gte: 2, $lte: 5 } } } ) ;


//-- "$nin" selects the documents where:
//      * the field value is not in the specified array or
//      * the field does not exist.

// Retrieve all the blog entries which are not commented by users "dragos" or "unknown"
db.first_collection.find( {"comments.user": { $nin:  ["dragos", "unknown"] } } ) ;


//===============================================================================
//                                        NULLs
//===============================================================================

//-- Filter { attribute : null } query matches documents that
//      * either contain the "atribute" whose value is null
//      * or that do not contain the cancelDate field.
// (Note: If the queried index is sparse, however, then the query will only
//      match null values, not missing fields)

// Retrieve blog entries having tag(s) with "null" value (we don't have any at this moment) and
//   blog entries with no tags
db.first_collection.find( { tags: null } )  ;

// Retrieve only blog entries having tag(s) with "null" value
db.first_collection.find({ tags : {"$in" : [null], "$exists" : true}})


//===============================================================================
//                              operator "$all"
//===============================================================================

//--    Which are the blog entries commented by both "valy" and "dragos" ?

// Next query retrives blog entries commented by "valy" OR "dragos":
db.first_collection.find( {"comments.user" : { "$in" : ["valy", "dragos" ]} } ) ;

// ... so does the folllowing query:
db.first_collection.find( {"comments.user" : "valy", "comments.user" :  "dragos" }  ) ;

//--    The corrent solution dpependds on operator "$all"
db.first_collection.find( {"comments.user" : { $all : ["valy", "dragos" ] }} ) ;


//===============================================================================
//                            operator "$size"
//===============================================================================

//--    "$size" returns the number of elements in an array

//--    Retrieve the blog entries having exactly two comments
db.first_collection.find({"comments" : {"$size" : 2}}) ;


//===============================================================================
//                           sort/order results
//===============================================================================

// First specify the author for three blog entries:
db.first_collection.update ( {title : "Virtualization and databases"} ,
	{"$set" : {author : "dragos"} }) ;
db.first_collection.update ( {title : "SQL la munte si la mare"} ,
	{"$set" : {author : "Fotache Marin"} }) ;
db.first_collection.update ( {title : "NoSQL Databases"} ,
	{"$set" : {author : "Valerica Greavu-Serban"} }) ;


// Display all the blog entries in order of the authors
db.first_collection.find().sort({author: 1}) ;
// Notice that first is the boook without author and also that
//  author "dragos" occurs after "Valerica Greavu-Serban" (because
//   of lowercase)

// Display all the blog entries in order of the authors; blog entries written by the same
//   author (or authors havibg identical names) will be ordered
//   by title, descendingly
db.first_collection.find().sort({author: 1, title: -1}) ;


//===============================================================================
//                     operator "$slice"
//===============================================================================

// First add a comment for the book "NoSQL Databases"
db.first_collection.update ( {"title" : "NoSQL Databases"},
	{ "$addToSet" : {"comments" :
		{"user" : "valy", "text" : "Quite Good!!", "votes" : 2 },
					 } }  ) ;

// Display the last two comments of that book
db.first_collection.findOne( {"title" : "NoSQL Databases"}, {"comments" : {"$slice" : -2}}) ;

// Display the first two comments of the same book
db.first_collection.findOne( {"title" : "NoSQL Databases"}, {"comments" : {"$slice" : 2}}) ;


//===============================================================================
//                     operator "$elemMatch"
//===============================================================================

//--    Use "$elemMatch" operator to specify multiple criteria on the elements of an array
//  such that at least one array element satisfies all the specified criteria.

// First, change the first comment of the book "SQL la munte si la mare"
db.first_collection.update ( {title : "SQL la munte si la mare"},
	{ "$set" : {"comments.0.text": "Strange" } } )  ;
// Also, change the third commenter of the book "SQL la munte si la mare"
db.first_collection.update ( {title : "SQL la munte si la mare"},
	{ "$set" : {"comments.2.user": "dragos" } } )  ;
// check
db.first_collection.find( ) ;


//--    Retrieve all the blog entries where user "dragos" commented "Strange"

// The following query extracts the blog entries for which there is a comment "Strange"
//   and a commenter "dragos"
db.first_collection.find( {"comments.user": "dragos", "comments.text": "Strange"}) ;
// The second returned document has "dragos" among commenters and "Strange"
//   among comments, but actually "dragos" commented differently ("Mediocre") that book

//      Solution requires "$elemMatch" operator
db.first_collection.find({"comments" : {"$elemMatch" : {user: "dragos", text: "Strange" } } } ) ;



//===============================================================================
//                                        Cursors
//===============================================================================

db.first_collection.find() ;

//===============================================================================
//--   Looping through records at which the cursor points to
//===============================================================================

// 1st version ("hasNext()")
var cursor = db.first_collection.find( {"title" : /database/i }) ;
while (cursor.hasNext() ) {
	obj = cursor.next() ;
	print(obj.title) ;
	}

// 2nd version ("forEach()")
var cursor = db.first_collection.find( {"title" : /database/i }) ;
cursor.forEach(function(x) {
	print(x.title) ;
	} ) ;

// 3rd version (enhanced 2nd version)
var cursor = db.first_collection.find( {"title" : /database/i }).sort( {"author" : 1}) ;
cursor.forEach(function(x) {
	print("title: " + x.title + ", author: " + x.author) ;
	} ) ;

// 4th version: 3rd version, but display only the first two documents
var cursor = db.first_collection.find( {"title" : /database/i }).sort( {"author" : -1}).limit(2) ;
cursor.forEach(function(x) {
	print("title: " + x.title + ", author: " + x.author) ;
	} ) ;

// 5th version: 3rd version, but display only the second and the third documents (skip the first doc)
var cursor = db.first_collection.find( {"title" : /database/i }).sort( {"author" : -1}).limit(2).skip(1) ;
cursor.forEach(function(x) {
	print("title: " + x.title + ", author: " + x.author) ;
	} ) ;



//------------------------------------------------------------------------------
//--    Requierement: Update documents, such as all the blog entries
//      containing "database" in the their title
//        to have the tag "databases"
//------------------------------------------------------------------------------
db.first_collection.find() ;

//--    We'll try a solution based on cursors
var cursor = db.first_collection.find( {"title" : /database/i }) ;
cursor.forEach(function(x) {
	db.first_collection.update ( {"title" : x.title},
		{ "$addToSet" : {"tags" : "databases" } }  ) ;
	} ) ;
// check
db.first_collection.find() ;


// use sdbis2022


//===============================================================================
//--         Working with two or more (logically related) collections
//===============================================================================

//          first collection - "counties"
db.counties.drop() ;
db.counties.save ( { _id : 'IS', countyName : 'Iasi', countyRegion  : 'Moldova' });
db.counties.insert ( { _id : 'B', countyName : 'Bucuresti'});
db.counties.save ( { _id : 'VN', countyName : 'Vrancea', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'NT', countyName : 'Neamt', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'SV', countyName : 'Suceava', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'VS', countyName : 'Vaslui', countyRegion  : 'Moldova' });
db.counties.save ( { _id : 'TM', countyName : 'Timis', countyRegion  : 'Banat'});
db.counties.save ( { _id : 'DJ', countyName : 'Dolj', countyRegion  : 'Oltenia'});

// create indexes
db.counties.createIndex({_id : 1}, {unique: true}) ;
db.counties.createIndex({countyName: 1}, {unique: true}) ;
db.counties.createIndex({countyRegion : 1}, {unique: false}) ;

db.counties.getIndexes()

// check
db.counties.find() ;


//          second collection - "postalCodes"
db.postalCodes.drop() ;
db.postalCodes.save ( { _id : '700505', cityTownVillage : 'Iasi', countyCode : 'IS' });
db.postalCodes.save ( { _id : '701150', cityTownVillage : 'Pascani', countyCode : 'IS' });
db.postalCodes.save ( { _id : '706500', cityTownVillage : 'Vaslui', countyCode : 'VS' });
db.postalCodes.save ( { _id : '705300', cityTownVillage : 'Focsani', countyCode : 'VN' });
db.postalCodes.save ( { _id : '706400', cityTownVillage : 'Birlad', countyCode : 'VS' });
db.postalCodes.save ( { _id : '705800', cityTownVillage : 'Suceava', countyCode : 'SV' });
db.postalCodes.save ( { _id : '705550', cityTownVillage : 'Roman', countyCode : 'NT' });
db.postalCodes.save ( { _id : '701900', cityTownVillage : 'Timisoara', countyCode : 'TM' });

// create indexes for this collection
db.postalCodes.createIndex({_id : 1}, {unique: true}) ;
db.postalCodes.createIndex({cityTownVillage: 1}, {unique: false}) ;
db.postalCodes.createIndex({countyCode: 1}, {unique: false}) ;

// check
db.postalCodes.find() ;


//--     There is a logical relationship between collections "counties" and
//  "postalCodes" (in relational database model this relationship involves a
//  referential integrity).
//  This is impossible to declare in Document DBMSs; we'll find out some options
//   for chaining the queries in order to retrieve information from multiple
//   collections

//------------------------------------------------------------------------------
//--    Requierement: show the county name and the region for city of Pascani
// Basic idea: retrieve any postal code in Iasi, then store the "countyCode" of
//   that document (associated to a postal/zip code) in a variable, and then
//   use the variable for filtering collection "counties"
//------------------------------------------------------------------------------

// retrieve any postal code for Pascani
row_postalCode = db.postalCodes.findOne ({cityTownVillage : 'Pascani'}) ;
// use the variable in a filter applied to collection "counties"
db.counties.find({'_id' : row_postalCode['countyCode']}) ;

// ...or ...
db.counties.find({'_id' : row_postalCode.countyCode}) ;


//  Caution: "findOne" works, whereas "find" does not!
row_postalCode = db.postalCodes.find({cityTownVillage : 'Pascani'}) ;
// use the variable in a filter applied to collection "counties"
db.counties.find({'_id' : row_postalCode['countyCode']}) ;
//  this query displays all of the counties, not only county where city of Pascani is located

//  A variable-document attribute can be stored into a scalar variable which further can serve as
//   an argument in function "find"
row_postalCode = db.postalCodes.findOne ({_id : '701150'}) ;
// initialize the scalar variable
vPostalCode = row_postalCode['countyCode'] ;
// use the variable in "find" command
db.counties.find({'_id' : vPostalCode }) ;


//===============================================================================
//--    Including result of a "find" as an argument of another "find"
//===============================================================================
//
// The most condensed version of a query for retrivieng the region for the townn of "Pascani"
//   includes "findOne" result into a "find" functions (so we do not need cursors and variables
db.counties.find({'_id' : db.postalCodes.findOne ({cityTownVillage : 'Pascani'}).countyCode }) ;



//------------------------------------------------------------------------------
//--    We already saw that:
//      * "find()" returns a cursor (reference) to a set of documents
//      * similar to SQL DBMS's, we cannot acces directly the cursor records, but we have to
//        load them sequentially into variables.
//------------------------------------------------------------------------------
//      Consequently, for displaying the county name and the region for city of Pascani,
//  one can write solutions based in cursors.
//------------------------------------------------------------------------------

//  (hasNext)
var myCursor = db.postalCodes.find ({_id : '701150'}) ;
var myRow = myCursor.hasNext() ? myCursor.next() :null ;
if (myRow) {
	var myCountyCode = myRow.countyCode ;
	print (myCountyCode) ;
	}
db.counties.find({'_id' : myCountyCode }) ;

//  (forEach)
var myCursor = db.postalCodes.find ({_id : '701150'}) ;
var  myCountyCode ;
myCursor.forEach(function(x) {
	myCountyCode = x.countyCode ;
	print(myCountyCode) ;
	} ) ;
db.counties.find({'_id' : myCountyCode }) ;


//------------------------------------------------------------------------------
//--    Requierement: Get all the the postal codes for cities,
//        towns and villages located in "Moldova "region
//------------------------------------------------------------------------------

//  Here it is a solution based on regular expressions
// retrieve all the counties in "Moldova" region
var myCursor = db.counties.find ({countyRegion  : 'Moldova'}) ;
// initialize a string variable in which the regular expression will be "built"
var myRegExp = "";
// "build" the regular expression by looping through documents referred by the cursor
myCursor.forEach(function(x) {
	var myCountyCode = x._id ;
	print(myCountyCode) ;
	if (myRegExp == "") {
		myRegExp = "^" + myCountyCode ; }
	else {
		myRegExp = myRegExp + "|^" + myCountyCode ; }
	print (myRegExp) ;
	} ) ;
// use the regulat expression (stored in variable "myRegExp") in "find"
db.postalCodes.find({'countyCode' : {"$regex" : myRegExp  } }) ;



//------------------------------------------------------------------------------
//--    Requirement: For each postal code in `Moldova` get a result which
//                 includes the county and region names
//------------------------------------------------------------------------------
// Hint: the query must produce something resembling the SQL query:
// SELECT * FROM postalCodes NATURAL JOIN counties


//---------------------------------
// 		   solution no. 1
//---------------------------------

// we'll get the result as a new collection: `temp`
db.temp.remove({}) ;

// get the counties in `Moldova` region
var counties_moldova = db.counties.find ({countyRegion  : 'Moldova'}) ;

// loop through these counties and get all the zip codes, each time ,
//   we'll inserting a document in `temp`
counties_moldova.forEach(function(x) {
	var crt_countyCode = x._id ;
	//print(myCountyCode) ;

	// the second cursor will get all the zip codes in each of the counties
	var crt_post_codes = db.postalCodes.find({'countyCode' : crt_countyCode}) ;
	crt_post_codes.forEach(function(y) {
	    // add the document containing information about the county into the
			//  current document (y) which is related to  a postal code
			y.county = x
			// insert the document into the resulting collection
			db.temp.insert(y)
		})
	} ) ;

// check the content of the result
db.temp.find() ;


//---------------------------------
// 		solution no. 2
//---------------------------------

// we'll get the result as a new collection: `temp`
db.temp2.remove({}) ;

// get the counties in `Moldova` region
var counties_moldova = db.counties.find ({countyRegion  : 'Moldova'}) ;

// loop through these counties and get all the zip codes, each time ,
//   we'll inserting a document in `temp`
counties_moldova.forEach(function(x) {
	var crt_countyCode = x._id ;
	//print(myCountyCode) ;

	// the second cursor will get all the zip codes in each of the counties
	var crt_post_codes = db.postalCodes.find({'countyCode' : crt_countyCode}) ;
	crt_post_codes.forEach(function(y) {
	    // add the document containing information about the county into the
			//  current document (y) which is related to  a postal code
			y.countyName = x.countyName
			y.countyRegion = x.countyRegion

			// insert the document into the resulting collection
			db.temp2.insert(y)
		})
	} ) ;

// check the content of the result
db.temp2.find() ;



//===============================================================================
//--                    cursor.toArray()
//--     The toArray() method returns an array that contains all the
// documents from a cursor.
//===============================================================================

//--    Display all the counties  located in "Moldova "region
// retrieve counties in "Moldova" region and store documents into an array
myArray = db.counties.find ({countyRegion : 'Moldova'}).toArray() ;
// display the array just by writing its name...
myArray ;
// ... or with the command "printjson"
printjson (myArray) ;

//--     Show the county name and the region for city of Pascani
// Store the results of "find" into an array variable ("myArray")
myArray = db.postalCodes.find ({_id : '701150'}).toArray() ;
// Use the array variable (only the first array element) in "find"
db.counties.find({'_id' : myArray[0].countyCode }) ;


//===============================================================================
//--                    `map` function
//--   Among other features, `map` allows the queries to returns
//     parts of the documents as arrays of values (not arrays of documents)
// -- This will be useful (see script `01-05b`) for some types of "subqueries"
//===============================================================================

// Get an array with all county names
db.counties.find().map( function(x) { return x.countyName; } );



//===============================================================================
//                          Counts and distincts (OPTIONAL)
//===============================================================================

// How many documents are there in collection "first_collection"?
db.first_collection.count() ;

// How many blog entries cover "sql"?
db.first_collection.count({"tags" : /sql/i})

// Retrieve all the authors for blog entries "collected" in "first_collection"
//  (SQL equivalent: "SELECT DISTINCT author FROM first_collection")
db.runCommand({"distinct" : "first_collection", "key" : "author"})
// ...or
db.first_collection.distinct( "author") ;
// Unfortunately, it is not possible to sort the extracted values
db.first_collection.distinct( "author").sort({"author" : 1}) ;
// this woould generate an error; solution: use aggregation framework (see next script)


// Retrieve all the tags declared for blog entries in "first_collection"
db.runCommand({"distinct" : "first_collection", "key" : "tags"})
// ... or
db.first_collection.distinct( "tags");

// Retrieve all the commenters
db.runCommand({"distinct" : "first_collection", "key" : "comments.user"})
//...
// ... or
db.first_collection.distinct( "comments.user");

// Retrieve all the commenters of the blog entries covering subject/tag "SQL"
db.first_collection.distinct( 'comments.user', { tags: "SQL" } )


// Get an array with all region names
db.counties.distinct("countyRegion")


//===============================================================================
//                            Grouping (OPTIONAL)
//===============================================================================

db.first_collection.find();

//--    Display the number of written blog entries for each author?
db.first_collection.group ({
	key : {author : true},
	initial : {"n_of_blog entries" : 0 } ,
	reduce : function(doc, aggregator) {
		aggregator.n_of_blog entries += 1 ;
		}
	} ) ;
