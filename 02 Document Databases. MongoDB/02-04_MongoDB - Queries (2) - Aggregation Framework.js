//===============================================================================
//                           Aggregation Framework

//===============================================================================
//--  some of the Examples are taken/inspired from the book 
//--    "MongoDB in action" by Banker et al., Manning Publication, 2016
//--   other examples are taken/inspired from the MongoDB online documentation 
//--     and most examples are completely uninspiring
//===============================================================================

//--    Set (if necessary) "ppbd2020" as current db
use ppbd2020

//===============================================================================
//                      Re-populate collection "books" 
db.books.remove({}) ;

/* first book */
db.books.insert({
    title : "Virtualization and databases",
    authors : ["Dragos Cogean"],
    publisher: "Polirom",
    release_date: ISODate("2013-05-02"),
    price: 35,
    comments : [ 
        { user : "nosql_ecstatic", text : "Awesome!", 
            votes : 5}, 
        { user : "Valerica Greavu-Serban", text : "Find me on Facebook!",
            votes : 2},  
        { user : "stromboli", text : "Strange!",
            votes : 3}, 
        { user : "Marin Fotache", text : "Good!",
             votes : 4}       ],
   tags : [ "databases", "cloud computing", "virtualization", "vmware", "hyper-v" ]              
    }) ;

/* second book */
db.books.insert({
    title : "Adventures in Databases",
    url : "http://feaa.uaic.ro/adventures-in-databases/",
    authors : ["Valerica Greavu-Serban", "Dragos Cogean"],
    publisher: "FEAA",
    release_date: ISODate("2013-11-30"),
    price: 25,
    comments : [ 
        { user : "nosql_ecstatic", text : "Awesome!", 
            votes : 5}, 
        { user : "Valerica Greavu-Serban", text : "Educație: Coerciția NU va fi niciodată Cheia Motivației!",
            votes : 0},  
        { user : "Marin Fotache", text : "Good!",
             votes : 4},  
        { user : "Dragos Cogean", text : "Good!",
             votes : 4}       ],
    tags : [ "partitioning",  "CAP", "NoSQL", "databases" ]
    } ) ;

/* third book */
db.books.insert({
    title : "SQL la munte si la mare",
    tags : [ "SQL", "NoSQL", "query languages", "OQL", 
        "C-SQL", "Hive", "databases"],
    authors : ["Fotache Marin"],
    publisher: "Polirom",
    release_date: ISODate("2014-02-28"),
    price: 45,
    comments : [ 
        { user : "nosql_ecstatic", text : "Awesome!", 
            votes : 5}, 
        { user : "Valerica Greavu-Serban", text : "Despre oameni: În umbra copacilor bătrâni cresc mușchi...",
            votes : 5},  
        { user : "Dragos Cogean", text : "Good!",
             votes : 4}       ]
    }) ;

/* fourth book */
db.books.insert({
    title : "NoSQL Databases",
    authors : ["Valerica Greavu-Serban", "Fotache Marin"],
    publisher: "Polirom",
    release_date: ISODate("2014-03-01"),
    price: 50,
    tags : [ "SQL", "NoSQL", "query languages",  
        "C-SQL", "databases", "Cypher", "Aggregation Framework"],
    comments : [ 
        { user : "nosql_ecstatic", text : "Awesome!", 
            votes : 5}, 
        { user : "Valerica Greavu-Serban", text : "Despre oameni: În umbra copacilor bătrâni cresc mușchi...",
            votes : 5},  
        { user : "Dragos Cogean", text : "Good!",
             votes : 4}       ]
    }) ;


//===============================================================================
//--		                Aggregation Framework
//--
//===============================================================================

//--    Agggregation Framework is the most powerful query mechanism  in MongoDb 

//--    There are three major sytax versions: 
// db.books.aggregate( <pipeline> )
// db.books.aggregate( [<pipeline>] )
// db.runCommand( { aggregate: "books", pipeline: [<pipeline>] } )



//===============================================================================
//--	Using Aggregation Framework (AF) for non-aggregate information :)
//===============================================================================

//
db.books.find() ;


//--    Retrieve all the books written or co-written by "Valerica Greavu-Serban"

// Basic solution based on "find"...
db.books.find({"authors" : "Valerica Greavu-Serban" }) ;
// ...and another one based on another AF syntax:
db.books.aggregate( [
	{ $match: { authors : "Valerica Greavu-Serban" }},            
        ] ) ;       


//-- For displaying only the title for the books (co)written by "Valerica Greavu-Serban"... 
db.books.find({"authors" : "Valerica Greavu-Serban" }, { "title" : 1, "_id" : 0 } ) ;
// ...we can use an AF query
db.books.aggregate( [
    { $match: { authors : "Valerica Greavu-Serban" }}, 
	{ $project : { _id : 0, title : 1 }} ,
        ] ) ;

        
//--    Retrieve, in uppercase and alphabetical order, all the book titles          
db.books.aggregate( [
	{ $project : { title : {$toUpper:"$title"} , _id:0 } },
	{ $sort : { title : 1 } } ] ) ;

        
//--    Retrieve publishers, in uppercase and alphabetically ordered
db.books.aggregate( [
	{ $project : { publisher : {$toUpper:"$publisher"} , _id:0 } },
	{ $sort : { publisher : 1 } } ] ) ;

// as "POLIROM" is repeated, we'll use "$group" as a DISTINCT operator:
db.books.aggregate( [
	{ $group : { _id: {$toUpper:"$publisher"} } },
	{ $project : { _id:1 } },
	{ $sort : { _id : 1 } } ] ) ;


?????
db.books.aggregate( [
	{ $group : { _id: {"publisher" :  $toUpper:"$publisher"} } },
	{ $project : { _id:1 } },
	{ $sort : { _id : 1 } } ] ) ;

db.books.aggregate( 
    { $match: { authors : "Valerica Greavu-Serban" }}, 
    { $group: { _id: {"author: Valerica Greavu-Serban" : null},
               n_of_books: { $sum: 1 } }} ) ;
  
        
//===============================================================================
//--	                        Basic agggregation
//===============================================================================

//==========================================================
//--                    COUNT          
        
//--    How many documents are there in collection "books"?
db.books.aggregate( [
   { $group: { _id: null,
               n_of_books: { $sum : 1 } } }
     ])


//-- A simpler solution, based on $count
db.books.aggregate( [
   { $count: "title"}
		] ) ;
   
// ...or
db.runCommand({ aggregate : "books", 
	pipeline : [  
		{ $group : { _id : null, 
				n_of_books : { $sum : 1 }
	}}]}) ;
 

//-- How many books were published by "Polirom"?
db.books.aggregate( [
	{ $match: { publisher : "Polirom" }},            
   	{ $group: { _id: "$publisher",
               n_of_books: { $sum: 1 } }}
        ] ) ; 
               
// ...or
db.runCommand({ aggregate : "books", 
	pipeline : [ 
		{ $match : { publisher : "Polirom" }}, 
		{ $group : { _id : "$publisher", 
				n_of_books : { $sum : 1 } }}
		] } )	;

//-- How many books were written by "Valerica Greavu-Serban"?
db.books.aggregate( 
    { $match: { authors : "Valerica Greavu-Serban" }}, 
    { $group: { _id: {"author: Valerica Greavu-Serban" : null},
               n_of_books: { $sum: 1 } }} ) ;
//-- The same requirement, but remove author name from the result
db.books.aggregate( 
    { $match: { authors : "Valerica Greavu-Serban" }}, 
    { $group: { _id: {"author: Valerica Greavu-Serban" : null},
               n_of_books: { $sum: 1 } }},
     {$project : { n_of_books:1,  _id: 0} }          
     ) ;
                
          
//-- How many books have an associated URL?
db.books.find( { url : {$exists : 1 } }) ;

db.books.aggregate( [
	{ $match: { url : {$exists : 1 } }},            
   	{ $group: { _id: null,
               n_of_books: { $sum: 1 } }}
        ] ) ; 
// ...or
db.runCommand({ aggregate : "books", 
	pipeline : [ 
		{ $match : { url : {$exists : 1 } }}, 
		{ $group : { _id : null, 
				n_of_books : { $sum : 1 } }}
		] } )	;

// we can name the group "_id", so that the result is more understandable
db.books.aggregate( 
	{ $match: { url : {$exists : 1 } }},            
   	{ $group: { _id: {n_of_books_with_associated_url: null},
               n_of_books: { $sum: 1 } }} ) ;          



//==========================================================
//--                    COUNT DISTINCT         

//-- How many publishers are there in the database/collection?

// -- 1st solution
db.books.aggregate( [
    { $group : { _id : "$publisher"}},
    { $count: "n_of_publishers"}
    ])   
         

// -- 2nd solution
db.books.aggregate( [
    { $group : { _id : "$publisher"}},
    { $group: { _id: null, n_of_publishers : { $sum : 1} } },
    { $project : {_id : 0}}
    ])   


               
//==========================================================
//--            Using $group for "proper" aggregation   
//--		     (like GROUP BY in SQL)      

//--    Display number of books for each publisher
db.books.aggregate( 
   { $group: { _id: {"publisher": "$publisher"}, 
            count: { $sum: 1 } } }, 
    { $sort : { _id : 1 } } ) ;       


//--    Display average book prices for each publisher
db.books.aggregate( 
   { $group: { _id: {"publisher": "$publisher"}, 
            averagePrice: { $avg: "$price" } } }, 
    { $sort : { _id : 1 } } ) ;       

        
//--    Display how many books cost less than 30 lei and how many more than 30
db.books.aggregate(
   { $group: { _id: {$cond: [ { $lte: [ "$price", 30 ] }, 0, 1 ]}, 
               n_of_books: { $sum: 1 } }} ) ; 

//... or, for a better look...
db.books.aggregate([
   { $group: { _id: { "Cost <= 30 RON (0=Yes, 1=No) " : {$cond: [ { $lte: [ "$price", 30 ] }, 0, 1 ]}}, 
               n_of_books: { $sum: 1 } }} ]) ; 

               
//--    Display how many books have an associated URL and how many haven't an URL 
/* http://stackoverflow.com/questions/25497150/mongodb-aggregate-by-field-exists  */
db.books.aggregate(
   { $group: { _id: {$gt: ["$url", null]}, 
               n_of_books: { $sum: 1 } }} ) ; 
//... enhanced version:
db.books.aggregate(
   { $group: { _id: {"With URL ": {$gt: ["$url", null]}}, 
               n_of_books: { $sum: 1 } }} ) ; 

               
//--    Display the number of books covering "NoSQL" for each publisher   
db.books.aggregate( 
   { $match: { tags : "NoSQL" } },            
   { $group: { _id: { "publisher (of NoSQL books)": "$publisher"},
               n_of_books: { $sum: 1 } } },
    { $sort: { _id : 1 } }     )	;
    
    
    
//============================================================
//--             Grouping by two attributes     

//-- Display number of books for each publsher and each year 
/*  (SELECT publisher, EXTRACT(year FROM release_date) AS year, COUNT(*) AS n_of_books 
    FROM books GROUP BY publisher, EXTRACT(year FROM release_date) )
    */
db.books.aggregate(
   { $group: { _id: { publisher: "$publisher",
                      year: { "$year": "$release_date" }},
               n_of_books: { $sum: 1 } } },
    { $sort: { _id : 1 } }   ) ;

    
//============================================================
//--             Group & filter (GROUP BY... HAVING...)   

//--    Display publishers with al least two published books covering "NoSQL" 
db.books.aggregate( 
   	{ $match: { tags : "NoSQL" } },            
   	{ $group: { _id: {"publisher": "$publisher"},
               n_of_books: { $sum: 1 } } },
    { $match: { n_of_books : { $gte : 2 } } },            
    { $sort: { _id : 1 } }     )	;

    
//=================================================================
//--   Something with no equivalence in SQL: retrieve collections
//        for each group

//      Display all the books published by each publisher        
db.books.aggregate( 
   { $group: { _id: "$publisher",
        "books: ": { $addToSet: "$title" } } }, 
   { $sort :{ _id: 1}}) ;    

   
//===============================================================================
//--	     Pivot tables "...a la Mongo": "$unwind" operator
//===============================================================================

//--    Retrieve, in uppercase and alphabetically ordered, all the authors name          
// Next query generates an error, so we need "$unwind"
db.books.aggregate( 
	{ $project : { name : {$toUpper:"$authors"} , _id:0 } },
	{ $sort : { name : 1 } }  ) ;

/* "$unwind" deconstructs (unnest) an array field from the input documents and 
    output a document for each array element. 
    Each output document replaces the array with an element value. 
    For each input document, the number of generated documents is the number of 
        array elements (can be zero for an empty array).        */
// see what result produces next query
db.books.aggregate( [ { $unwind : "$authors" } ] )

//--    Now, returning to the required information:
// Retrieve, in uppercase and alphabetically ordered, all the authors name          
db.books.aggregate( 
   { $unwind: "$authors" } ,
   { $project : { author: {$toUpper: "$authors"}, _id: 0  }},
   { $sort: {author: 1 }}) ;       
        
// Next solution extracts no duplicates
db.books.aggregate( 
    { $unwind: "$authors" } ,
    { $group : { _id: {$toUpper:"$authors"} } },
    { $project : { _id: 1 } },
    { $sort : { _id: 1 } } ) ;

    
//--    How many books were written by each author?
db.books.aggregate( 
    { $unwind: "$authors" } ,
    { $group : { _id: {"author": "$authors"},
               n_of_books: { $sum: 1 } }}) ;    

               
//--    Display the books written by each author?
db.books.aggregate( 
    { $unwind: "$authors" } ,
    { $group : { _id: {"author": "$authors"},
        "books: ": { $addToSet: "$title" } } }, 
    { $sort : { _id: 1 } } ) ;


//--    Display the number of books in which every tag is declared 
db.books.aggregate( 
    { $unwind: "$tags" } ,
    { $group : { _id: {"tag": "$tags"},
               n_of_books: { $sum: 1 } }},
    { $project: {tag: 1, n_of_books: 1 } },   
    { $sort :{ _id: 1}}) ;    

    
//--    Display the books for which each tag is declared 
db.books.aggregate( 
    { $unwind: "$tags" } ,
    { $group : { _id: {"tag": "$tags"},
        "books: ": { $addToSet: "$title" } } }, 
    { $sort : { _id: 1 } } ) ;


//--    Find the overall number of votes
// as "votes" is an attribute of "comments" (which is an array)  "comments" must be unwinded
db.books.aggregate( 
   { $unwind: "$comments" } ,     
   { $group: { _id: null, "votes_overall": { $sum: "$comments.votes" } } } ) ;


//--    Display the number of received votes by the comments of each book
db.books.aggregate( 
   { $unwind: "$comments" } ,     
   { $group: { _id: "$title",
        "n_of_votes": { $sum: "$comments.votes" } } }, 
   { $sort :{ _id: 1}}) ;    


//--    Number of posts written by each commenter (user)
db.books.aggregate(
   { $unwind: "$comments" },
   { $group: { _id: {"user": "$comments.user"},
         n_of_posts : { $sum: 1 } } },
    { $sort: {user: 1 } } ) ;       

 
//--    Display commenters with at least three posts 
db.books.aggregate(
   { $unwind: "$comments" },
   { $group: { _id: {"user": "$comments.user"},
         n_of_posts : { $sum: 1 } } },
    { $match: {n_of_posts: {$gte :3}}}, 
    { $sort: {n_of_posts: -1 } } ) ;       

    
//--    Display top 3 of commenters (in terms of number of posts)
db.books.aggregate(
   { $unwind: "$comments" },
   { $group: { _id: {"user": "$comments.user"},
         n_of_posts : { $sum: 1 } } },
   { $sort: {n_of_posts:  -1} },
   { $limit: 3} ) ;       


//===============================================================================
//              		Advanced Pipelines in Aggregation Framework
//    Wel'll pass the result of an "unwind" operator to another "unwind"    
db.books.aggregate( 
   { $unwind: "$authors" },
   { $unwind: "$comments" }
     )

   
//--    Display for each author the number of comments and the sum of votes for comments
//  posted for his/her books   
db.books.aggregate( 
   { $unwind: "$authors" },
   { $unwind: "$comments" },
   { $group: { _id: { author: "$authors"},
        n_of_posts: { $sum: 1 } ,
        sum_of_votes : { $sum: "$comments.votes" } } },
    { $sort: {_id: 1} }) ;       


db.books.find()




/* for additional examples of using Aggregation Framework ( "$out" and 
  "$lookup" operators, different functions for dealing with numbers, strings ans dates), and also
	examples that deal with  "normalized" collections -
	see script "02-05_MongoDB - Case study - Sales.js"       */

