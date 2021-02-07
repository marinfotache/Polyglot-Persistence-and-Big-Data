//===============================================================================
//                           Managing collections in MongoDB


//===============================================================================
//-- Disclaimer:
//--  some of the Examples are taken/inspired from the book
//--    "MongoDB in action" by Banker et al., Manning Publications, 2016
//--   other examples are taken/inspired from the MongoDB online documentation
//===============================================================================

//--    Set (if necessary) "ppbd2021" as current db
use ppbd2021

//--    Remove (if necessary) all documents in collection "first_collection"
db.first_collection.remove({}) ;

//--    Insert documents, this time using variables
x1 =  { title: 'I Like Databases',
		text_comments: ['Good!', 'Excellent', 'Mediocre', 'Disgusting' ]} ;
db.first_collection.insert(x1) ;


x2 = { titlel: "SQL la munte si la mare",
		text_comments: ['Boring', 'Disgusting', 'Mediocre' ]} ;
x3 = { title: "NoSQL Databases",
		text_comments: ['Good!', 'Boring', 'Mediocre' ]} ;
x4 = { title: "Virtualization and databases",
		comments: [
			{ user: 'iolanda', text: 'Awesome!'},
			{ user: 'valy', text: 'Find me on Facebook! Always!' },
			{ user: 'dragsos', text: 'Strange' },
			{ user: 'marin', text: 'Mediocre'}
                            ]
	} ;

db.first_collection.insert(x2) ;
db.first_collection.insert(x3) ;
db.first_collection.insert(x4) ;

//--    Retrieve all the documents (check the inserts)
db.first_collection.find() ;


//-- alternatively, we can insert bulks of documents
array1 = [
    // first document
    { titlel: "Fatigue Syndrome at SIA and SDBIS students",
		text_comments: ['Good', 'Finally, somebody understands us!']},
    // second document
    { title: "An Essay (and 100 Reasons) on Why SIA Students Do Not Attend the Master Courses" },
    // third document
    { title: "Being Optimistic (Almost) Every Day",
		comments: [
			{ user: 'iolanda', text: 'Awesome!'},
			{ user: 'valy', text: 'Find me on Facebook!' },
			{ user: 'suicidal', text: 'Awesome!' }
                            ]
	}
      ]  ;

//-- `insertMany` will return ObjectId 's for all inserted documentes
db.first_collection.insertMany(array1) ;
// Note: the array could be inserted also with `insert` command, but no ObjectId will be provided
//  as a results of inserts (of course, ObjectId 's exist in the collecttion)



//--  Retrieve all the documents (check the inserts)
db.first_collection.find() ;


//===============================================================================
//                              Now, for the updates
//===============================================================================

//=======================================================
//              A begginer's mistake with "update"
//
//--    DO NOT replace entire documents !
//  The next update is wrong (we want to add some tags for the book "I Like Databases")
db.first_collection.update( {title: "I Like Databases"},
	 {tags : ['databases', 'mongodb', 'indexing'] }) ;

// The above update command removed all the other attributes of the document (keeping only "tags")
db.first_collection.find( {title: "I Like Databases"}) ;
// nothing is displayed since the title was removed (only ObjectId and attribute "tags" were kept)
db.first_collection.find() ;

//=======================================================
//     The same problem with function "findAndModify"

//--    Restore the "damaged" document
// first, delete it
db.first_collection.remove( { tags : ['databases', 'mongodb', 'indexing'] }) ;

//-- reinsert it
x1 =  { title: 'I Like Databases',
		text_comments: ['Good!', 'Excellent', 'Mediocre', 'Disgusting' ]} ;
db.first_collection.insert(x1) ;
// check
db.first_collection.find() ;

//
//--     "findAndModify" modifies and returns a single document
//
// Example: we want to add some tags for the book "I Like Databases"
db.first_collection.findAndModify({
    query: {title: "I Like Databases"},
    update: {tags : ['databases', 'mongodb', 'indexing']} ,
    new: true,
    upsert: true
});
// check
db.first_collection.find() ;
// you can notice the same mistake as in previous "update"


//=============================================================================
//           A simple solution basen on "save" and a variable

//--    Restore the "damaged" document
// first, delete it
db.first_collection.remove( { tags : ['databases', 'mongodb', 'indexing'] }) ;
//-- reinsert it
x1 = { title: 'I Like Databases',
		text_comments: ['Good!', 'Excellent', 'Mediocre', 'Disgusting' ]} ;
db.first_collection.insert(x1) ;
// check
db.first_collection.find() ;
//
// load the document into a variable
var book = db.first_collection.findOne( {title: "I Like Databases"}) ;
// add attribute "tags" in variable "book"
book.tags = ['databases', 'mongodb', 'indexing'] ;
// "save" command works as an upsert: if the document exists (based on its ObjectId) in
//   the collection, "save" acts as an "update"; otherwise "save" acts like an "insert"
db.first_collection.save(book)
// check
db.first_collection.find() ;


//=======================================================
//              Operator $set

//--    Restore (again) the document
// first, delete it using its title
db.first_collection.remove( {title: "I Like Databases"} )
//-- reinsert it
x1 =  {  title: 'I Like Databases',
		text_comments: ['Good!', 'Excellent', 'Mediocre', 'Disgusting' ] } ;
db.first_collection.insert(x1) ;

// check
db.first_collection.find() ;


//--    For adding an attribute in a document, operator "$set" is required

// Add tags for the book whose title is 'I Like Databases'
db.first_collection.update( {title: "I Like Databases"},
	 {"$set" : {tags : ['databases', 'mongodb', 'indexing'] }}) ;
// Check the update
db.first_collection.find( {title: "I Like Databases"}) ;


//  The same solution can be applied for "findAndModify"
//
//--    Restore (again) the document
// first, delete it using its title
db.first_collection.remove( {title: "I Like Databases"} ) ;
//-- reinsert it
x1 =  { title: 'I Like Databases',
		text_comments: ['Good!', 'Excellent', 'Mediocre' ]} ;
db.first_collection.insert(x1) ;
// Check the update
db.first_collection.find( {title: "I Like Databases"}) ;


//  "findAndModify" modifies and returns a single document
db.first_collection.findAndModify({
    query: {title: "I Like Databases"},
    update: {$set: {tags : ['databases', 'mongodb', 'indexing']} },
    new: true,
    upsert: true
});
// check
db.first_collection.find() ;


//--   Remove the tags for "I Like Databases"
db.first_collection.update( {title: "I Like Databases"},
	 {"$unset" : {tags : 1} }) ;
// Check the update
db.first_collection.find( {title: "I Like Databases"}) ;


//=========================================================================================
//              Variables, a function and a loop

//--   Next, we want to change the structure for document whose title is "SQL la munte si la mare"
// As you might noticed, instead of "title" we typed "titlel" which could be "nightmarish" when
// querying the collection (many books and papers on schema-lessness keep mum about this).
// Consequently, we must change the attribute name, from "titlel" to "title".
// Additionally, we want to split, for all of the documents, the attribute "text_comments"
//    into two properties, "comments.user" and "comments.text".
//      Well, there is no DDL in Mongo, so we'll write an simple program in JavaScript.

// first store the document into composite variable "d1"
var d1 = db.first_collection.findOne({titlel : "SQL la munte si la mare"});
// display the variable
d1

// add property "comments" to the variable; we do not the commentor; but the text
//  of the comment is taken from attribute "text_comments"
d1.comments = { user : "unknown", text : d1.text_comments };

// add property "title", since we (still) do not know any option for renaming attributes
d1.title = d1.titlel;

// remove the wrong attribute "titles"
delete d1.titlel;

// also remove "text_comments"
delete d1.text_comments;

// display the variable
d1 ;

// now replace the entire document with variable "d1"
db.first_collection.update({titlel : "SQL la munte si la mare"}, d1);
// check
db.first_collection.findOne({title : "SQL la munte si la mare"}) ;


//--    With function "$rename" one can easily change the name of the attribute

// first, reverse the last update...
db.first_collection.update({title: "SQL la munte si la mare"},
	{ $rename: { "title": "titlel" } } );

// ... check
db.first_collection.findOne({titlel : "SQL la munte si la mare"}) ;

// second, rename again:
db.first_collection.update({titlel: "SQL la munte si la mare"}, { $rename: { "titlel": "title" } } );

db.first_collection.update({}, { $rename: { "titlel": "title" } } );

// check
db.first_collection.findOne({title : "SQL la munte si la mare"}) ;


//--   It worked, but not as planned (see "comments" - we wanted a pair of "user"/"text"
// properties for each comment, not one user for all of the comments.

//--    We'll try another solution

// variable "d2" gets the current content of the document
d2 = db.first_collection.findOne({title : "SQL la munte si la mare"}) ;

// another variable (an array) - "texts" - gets the text for all of the comments
texts = d2.comments.text ;

// declare a composite variable called "a_comm"
a_comm = new Object() ;

// declare two properties for this newly added variable
a_comm.user = "an user" ;
a_comm.text =  "a text " ;

// check (print) the variable
a_comm

// now define a function - "a_comment" - that will be used as a
//  "set" function for variable "a_comm"
function a_comment( user, text ) {
	    this.user = user;
	    this.text = text;
}

// declare an array variable  - "good_comm"
var good_comm = new Array() ;

// next loop walks through the array "texts" and sets values for variable "good_comm"
for (var i = 0 ; i < texts.length ; i++) {
	good_comm[i] = new a_comment( "unknown", texts[i] );
}
// store "good_comm" into "d2.comments"
d2.comments = good_comm ;

// replace existing document in the collection with variable "d2"
db.first_collection.update({title : "SQL la munte si la mare"}, d2);

// now it is ok
db.first_collection.findOne({title : "SQL la munte si la mare"}) ;


//=======================================================
//              Operator "$inc"

//--   "$inc" is useful for incrementing an attribute value

// first, add the attribute "num_of_comm" for the same document (book)
//... declare variable "d3"
d3 = db.first_collection.findOne({ title: "SQL la munte si la mare"}) ;
//... add the attribute to the variable
d3.num_of_comm = 0 ;
//... replace the orginal document with variable content
db.first_collection.update({ title: "SQL la munte si la mare"}, d3);
//  Increment (by 1) the number of comments for that book
db.first_collection.update({title: "SQL la munte si la mare"},
	{"$inc" : {num_of_comm : 1}}) ;
// check
db.first_collection.findOne({title : "SQL la munte si la mare"}) ;


//=======================================================
//              Operator $set (reprise)

//--    Updating a document through variables (as seen above) is awkward.
// "$set" makes update operation more "SQL-ish"

// Task: in the document describing the book "I Like Databases"
//   add an attribute - "url"
db.first_collection.update ( {title: "I Like Databases"},
	{"$set" : {url: "http://example.com/databases.txt"} } ) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"}) ;

// Add two more attributes - ("vote_count" and "author") - in the same document
db.first_collection.update ( {title: "I Like Databases"},
	{"$set" : {vote_count : "20" }    } ) ;
db.first_collection.update ( {title: "I Like Databases"},
	{"$set" : {author : "Valy Greavu"}  } ) ;
// check
db.first_collection.find({title: "I Like Databases"})

// Update author of the same book
db.first_collection.update({title: "I Like Databases"},
	{"$set" : {author : "Valerica Greavu-Serban"}}) ;
// check the update
db.first_collection.find({"title": "I Like Databases"})

// Now we'll add an array attribute ("comments") in the same document
db.first_collection.update( {title : "I Like Databases"},
	{"$set" : {comments :  [
		{ user: "bjones",  text: "Interesting article!"  },
		{ user: "blogger", text: "Another related article is at http://example.com/db/db.txt" }
			        ] } } ) ;
// check
db.first_collection.find({title: "I Like Databases"})

// remove old comments ("text_comments") of this book with operator "$unset"
db.first_collection.update({title : "I Like Databases"},  {"$unset" : {text_comments: 1 }})
// check
db.first_collection.find({"title": "I Like Databases"})

//--    Next, change the author of the second comment in the document representing
// the book "I Like Databases"

// Using...
db.first_collection.findOne({title: "I Like Databases"}) ;
// we notice that the second "commenter" is "blogger"
db.first_collection.findOne({title: "I Like Databases"}, {"comments.user": "blogger"} );

// Now we want to change the user for that comment, from "blogger" to "panda"
db.first_collection.update ( {title: "I Like Databases", "comments.user": "blogger"},
	{"$set" : {"comments.user": "panda"  }  }) ;
// DOES NOT WORK !
//  cannot use the part (comments of comments.user) to traverse the element
//    ({comments: [ { user: "bjones", text: "Interesting article!" },
//    { user: "blogger", text: "Another related article is at http://example.com/db/db.txt" } ]})

// Solution is simple, but wait until the sub-section
//   Operator "arrayAttribute.$.property"


//===============================================================================
//                              Operations with arrays
//===============================================================================

db.first_collection.find({"title" : "I Like Databases"})
//=======================================================
//                    Operator "$push"

//--    Adding an array element


// Add a comment in document "I Like Databases"
//  operator "$push" does the trick
db.first_collection.update ( {title: "I Like Databases"},  {$push : { comments:
	{user: "dragos", text: "well, not bad at all!"} } }) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;

db.first_collection.update ( {title: "I Like Databases"},  {$push : { comments:
	{user: "vasile", text: "so boring!"} } }) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;


// Now, in the same document, add an array attribute ("tags") with one element ("NoSQL")
db.first_collection.update( {"title" : "I Like Databases"},
	{"$set" : {"tags" :  ["NoSQL"] } } ) ;
//... then add another tag
db.first_collection.update ( {"title" : "I Like Databases"}, {$push : { "tags" : "replication" }})
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;

// In the same document add (in just one move) two tags
db.first_collection.update ( {"title" : "I Like Databases"}, {$push : { "tags" : ["sharding", "partitioning"] }}) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;
// Oops! We did it wrong, since instead of two new tags, just one (of an array type) was added;
// consequently, we have to delete all of the tags and then re-add them properly
db.first_collection.update({"title" : "I Like Databases"},  {"$unset" : {"tags" : 1}}) ;
db.first_collection.update ( {"title" : "I Like Databases"}, {$push : { "tags" : "NoSQL" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"}, {$push : { "tags" : "replication" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"}, {$push : { "tags" : "sharding" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"}, {$push : { "tags" : "partitioning" }}) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;

//--    But there is a more elegant solution - by using "$push" with option "each"
// delete the tags
db.first_collection.update({"title" : "I Like Databases"},  {"$unset" : {"tags" : 1}}) ;
// check the deletion
db.first_collection.findOne({"title" : "I Like Databases"} ) ;
// now the recommended solution:
db.first_collection.update ( {"title" : "I Like Databases"}, {$push :
	{ "tags" : { $each : ["NoSQL" , "replication", "sharding", "partitioning", "NoSQL", "NoSQL" ]}}}) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;
// notice that tag "NoSQL" is duplicated (as specified in update)
// to prevent duplicates in arrays, use "addToSet"


//=======================================================
//                    Operator "$addToSet"

//--   Unlike "$push", operator "$addToSet" checks for duplicates
//   before adding elements in an array
// Remove all of the document tags
db.first_collection.update({"title" : "I Like Databases"},  {"$unset" : {"tags" : 1}}) ;
// Add successively the tags in the document
db.first_collection.update ( {"title" : "I Like Databases"},
	{$addToSet : { "tags" : "partitioning" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"},
	{$addToSet : { "tags" : "CAP" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"},
	{$addToSet : { "tags" : "NoSQL" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"},
	{$addToSet : { "tags" : "NoSQL" }}) ;
db.first_collection.update ( {"title" : "I Like Databases"},
	{$addToSet : { "tags" : "NoSQL" }}) ;
// check
db.first_collection.findOne({"title" : "I Like Databases"} ) ;
// Even if we tried to add "NoSQL" three times, there is only one occurence of this tag


//--   Like "$push", with "$addToSet" we can add more elements in an array within a single operation

//--    Without option "$each", the added attribute is an array of arrays
// Take a document...
db.first_collection.findOne ( { "title": "SQL la munte si la mare"} ) ;
// ... and try to add three tags using "$addToSet"
db.first_collection.update ( { "title": "SQL la munte si la mare"},
	{ $addToSet : {"tags" : ["SQL", "NoSQL", "query language"]} } ) ;
// check
db.first_collection.findOne ( { "title": "SQL la munte si la mare"} ) ;
// As expected, after the last update, attribute "tags" is an array of arrays

// ... so we remove all the tags...
db.first_collection.update({"title" : "SQL la munte si la mare"},  {"$unset" : {"tags" : 1}}) ;
// ... and add them but this time using "each"
db.first_collection.update ( { "title": "SQL la munte si la mare"},
	{ $addToSet : {"tags" : {"$each" : ["SQL", "NoSQL", "query languages"] }  } } ) ;
// Now, it's better.
db.first_collection.findOne ( { "title": "SQL la munte si la mare"} ) ;


//=======================================================
//                    Operator "$pull"

//--  First we add some more tags (for subsequent deletions)
db.first_collection.update ( { "title": "SQL la munte si la mare"},
	{ $addToSet : {"tags" : {"$each" : ["relational algebra", "OQL", "C-SQL"] }  } } ) ;
db.first_collection.update ( { "title": "SQL la munte si la mare"},
	{ $addToSet : {"tags" : {"$each" : ["relational algebra", "OQL", "Hive"] }  } } ) ;
// check
db.first_collection.findOne ( { "title": "SQL la munte si la mare"} ) ;


//--   Deleting individual elements in an array is possible using "$pull"
// Ex: for book "SQL la munte si la mare" we want to delete only the tag "relational algebra"
db.first_collection.update ( {"title" : "SQL la munte si la mare"},
			{"$pull" : {"tags" : "relational algebra"} } ) ;
// check
db.first_collection.findOne ( { "title": "SQL la munte si la mare"} ) ;


//=======================================================
//             Operator "arrayAttribute.index.property"
//  (changing element of an array when knowing the element index)

// Take another document (book)
db.first_collection.findOne( { "title" : "NoSQL Databases"} ) ;

//--   Add some comments for which there is a third attribute storing the number of votes ("votes")
db.first_collection.update ( {"title" : "NoSQL Databases"},
	{ "$set" : {"comments" : [
		{"user" : "dragos", "text" : "Good!", "votes" : 3 },
		{"user" : "bjones", "text" : "Mediocre", "votes" : 1 } ]  } }  ) ;
// check
db.first_collection.findOne( { "title" : "NoSQL Databases"} ) ;
// remove old comments attribute ("text_comments")
db.first_collection.update ( {"title" : "NoSQL Databases"}, { "$unset" : { "text_comments"  :1 } } ) ;
// check
db.first_collection.findOne( { "title" : "NoSQL Databases"} ) ;

//--   Increment by 1 the number of votes for the second comment.
// Note that the second element (comment) has index 1 (array indexes start with 0)
db.first_collection.update ( {"title" : "NoSQL Databases"},
	{ "$inc" : {"comments.1.votes" : 1 } } ) ;
// check with...
db.first_collection.findOne( { "title" : "NoSQL Databases"} ) ;
// ...or viewing only attribute "comments" (and the ObjectId)
db.first_collection.find( { "title" : "NoSQL Databases"}, {"comments" : 1} ) ;


//=======================================================
//             Operator "arrayAttribute.$.property"
//  (changing element of an array when not knowing the element index)

//--   This option is quite useful when the array has many elements and knowing the
// element index proves to be difficult

//  This is the document we are interested in
db.first_collection.findOne( { "title" : "Virtualization and databases"} ) ;

//--    One of the comments was written by user "dragsos"; we want to change it into "dragos"

// For doing that, when qualifying, between the attribute name ("comments") and its (sub)property
//   ("user") a "$" is inserted
db.first_collection.update ( {"title" : "Virtualization and databases", "comments.user" : "dragsos" },
	{ "$set" : {"comments.$.user" : "dragos" } } ) ;

// check by retrieving the document describing the book "Virtualization and databases"
db.first_collection.findOne( { "title" : "Virtualization and databases"} ) ;

//... or by retrieving all the books for which there is a comment
// written by ("comments.user) "dragos"
db.first_collection.find ( {"comments.user" : "dragos" }) ;


//===============================================================================
//                                      Upserts
//===============================================================================
//      Unlike SQL, "update" in MongoDB is actually an "upsert" - an "insert"
// combined with an "update"

//--     General syntax:
// db.collection.update(
//   <query>,
//   <update>,
//   { upsert: <boolean>, multi: <boolean> }
// )

//--    Add attribute "votes" in  ALL of the documents of collection "first_collection"
db.first_collection.update ({ }, {$set : {"votes" : 0}}, false, true) ;
// check
db.first_collection.find ( ) ;


//--    Something more interesting:
//      For all the document with tag "NoSQL" we add also tag "databases"
db.first_collection.update ({ "tags" : "NoSQL" }, {$addToSet : {"tags" : "databases"}}, false, true) ;
db.first_collection.find ( ) ;
