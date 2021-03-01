//===============================================================================
//                            (Very) Basics operations with MongoDB
//===============================================================================
// For installation of MongoDB Community Server and Robo 3T, select presentation:
// https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/01%20Document%20Databases.%20MongoDB/01%20Document_DB__MongoDB.pptx


//--   An excellent MongoDB client (we'll use it this semester): Robo 3T (free)
// http://robomongo.org/

//--   In order to launch a commad or group of commands in Robomongo/Robo 3T
// select commang/gruoup (by mouse/keyboard) and then press Ctrl + Enter keys
// (on Mac systems the key combination is cmd + Enter)

//--    Display all available command for working with databases
db.help()

//--    show existing databases
show dbs
// ...or
show databases

//--    show current database (from now we'll use mainly "db") in comments
db

//--    show some information/stistics about current db
db.stats()

//--    change/set the current db
use sales

//--    set "ppbd2021" as current bd
use ppbd2021

//--    display existing collections (of documents) in the current db
show collections

use sales2021


//--    count the number of documents in a collection
db.first_collection.count()

//--    retrieve one document (record) in a collection
db.first_collection.findOne()


//===============================================================================
//                 Basics operations with documents and collections
//===============================================================================
//--  some of the Examples are taken/inspired from the book
//--    "MongoDB in action" by Banker et al., Manning Publication, 2016
//--   other examples are taken/inspired from the MongoDB online documentation
//===============================================================================

//-- we want to create a database named "ppbd2021"
//--    there is no such a commad "create database"
//--    instead we will just set "ppbd2021" as current database (even it is not created!)
use ppbd2021


//--    Display available commands
db.ppbd2021.help()

//--    Remove collection "first_collection" from current db ("bda")
db.first_collection.drop()




//--    Create collection "first_collection" (in current db).
// Notice that there is no CREATE COLLECTION command; instead the collection is created
//  at the moment of the first "insert"
db.first_collection.insert(
	{
	title: 'I Hate Databases',
	url: 'http://first_collection-unreal-example.com/ihatedatabases.html',
	author: 'gigel',
	vote_count: 20,
	tags: ['databases', 'mongodb', 'indexing'],
	image: {        url: 'http://first_collection-unreal-example.com/photo1.jpg',
                  caption: '',
                  type: 'jpg',
                  size: 75381,
                  data: "Binary"     },
	comments: [
    	{ user: 'wolfram_vasile', text: 'Interesting article!' },
      { user: 'xenon_iolanda', text: 'A similar article can be found at http://save-sdbis-from-databases.com/nervous-breakdown.html' }             ]
    }
        ) ;

//--    Since the is only one document in the collection, commands findOne() and find()
// will have the same result

//
db.first_collection.find() ;
//  SQL equivalent:
// SELECT * FROM first_collection

db.first_collection.findOne() ;
//  SQL equivalent:
// SELECT * FROM first_collection LIMIT 1



//--    Insert the second document (record) into collection "first_collection"
//-- insertOne will return the ObjectId
db.first_collection.insertOne(
	{
	title: 'SQL la munte si la mare',
	url: 'http://portal.feaa.uaic.ro/sqlmm.txt',
	author: 'mfotache',
	vote_count: 5,
	tags: ['databases', 'sql'],
	image: {
		url: 'http://portal.feaa.uaic.ro/sqlmm1.jpg',
		caption: '',
		type: 'jpg',
		size: 5381,
		data: "Binary"
		},
	comments: [
		{ user: 'gigel', text: 'Hating it, too!' },
		{ user: 'greavy', text: 'Find me on Facebook!' }
                    ]
                }
        ) ;

//--    Retrieve the most recent (second) inserted document
db.first_collection.find({title : "SQL la munte si la mare"}) ;

//  SQL equivalent:
// SELECT * FROM first_collection WHERE title = 'SQL la munte si la mare'


//--    Update the most recent document; in this document we add property (field) "country_author"
db.first_collection.update({title: "SQL la munte si la mare"}, {$set: {country_author: "Romania"}}) ;

// SQL pseudo-equivalent:
// ALTER TABLE first_collection ADD country_author....
// UPDATE first_collection SET country_author = 'Romania' WHERE title = 'SQL la munte si la mare'


//--      Now check if the update worked properly
// ... with "find"
db.first_collection.find({title: "SQL la munte si la mare"}) ;
// ... or "findOne"
db.first_collection.findOne({title: "SQL la munte si la mare"}) ;


//--    Retrieve all the blog entries where tag "databases" occurs
db.first_collection.find({'tags': 'databases'});

//--    Retrieve all the blog entries where tag "databases" occurs AND
// image size is greater than 1000.
//  Imagine size is stored by property "image.size" and operator
// "greater than" is "$gt"
db.first_collection.find({'tags': 'databases', 'image.size': {'$gt': 1000}});

//--    Sometimes none of the documents fulfills the filter condition...
db.first_collection.find({'tags': 'databases', 'image.size': {'$gt': 100000000}});


//--    Similar to operator "Project" in relational algebra and SQL,
//        we want to display, from all the documents within the collection,
//        only property/attribute/field "title";
//  by default, object id is dispayed
db.first_collection.find (
		{},   											// filter (record filtering)
		{title : true })  ;         // column/attribute selection

//  SQL equivalent:
// SELECT title FROM first_collection


//--    The same query as previous one, but this time without the object id
db.first_collection.find ({}, {title : true, _id : false } )  ;


//--    Remove all the documents commented by user "greavy";
// notice the notation "comments.user"
db.first_collection.remove({'comments.user' : 'greavy'}) ;
// checkProgram(
db.first_collection.find()

//  SQL equivalent:
// DELETE FROM first_collection WHERE first_collection.comments.user = 'greavy'



//--    Remove all the documents of collection "first_collection"
db.first_collection.remove({}) ;

//  SQL equivalent:
// DELETE FROM first_collection

//--    Now that we have nothing left, take a break !
db.first_collection.find()
