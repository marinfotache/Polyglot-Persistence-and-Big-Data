//===============================================================================
//  Aggregation Framework (The High-Level Query Language for MongoDB databases)
//===============================================================================
// last update: 2021-03-01

//===============================================================================
//--  some of the Examples are taken/inspired from the book
//--    "MongoDB in action" by Banker et al., Manning Publication, 2016
//--   other examples are taken/inspired from the MongoDB online documentation
//--     and most examples are completely uninspiring
//===============================================================================

//--    Set (if necessary) "ppbd2021" as current db
//use ppbd2021
// or
// use bigdata

//===============================================================================
//  Populate a slightly different (from `first_collection`) collection: `books`
//===============================================================================

db.books.remove({}) ;

/* first book */
db.books.insert({
    title : "Virtualization and databases",
    authors : ["Dragos Cogean"],
    publisher: "Polirom",
    release_date: ISODate("2013-05-02"),
    price: 35,
    quantity_sold: 100,
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
    quantity_sold: 1000,
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
    quantity_sold: 85,
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
    quantity_sold: 120,
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
//--
//--                          Aggregation Framework
//--
//===============================================================================

//--    Agggregation Framework is the most powerful query mechanism  in MongoDb

//--    There are three major sytax versions:
// db.books.aggregate( <pipeline> )
// db.books.aggregate( [<pipeline>] )
// db.runCommand( { aggregate: "books", pipeline: [<pipeline>] } )



//===============================================================================
//--                   Aggregation Framework (AF) Basics
//===============================================================================

//
db.books.find() ;

//------------------------------------------------------------------------------
//--  Retrieve all the books written or co-written by "Valerica Greavu-Serban"
//------------------------------------------------------------------------------

// Basic solution based on "find"...
db.books.find({"authors" : "Valerica Greavu-Serban" }) ;

// ...and another one based on another AF syntax:
db.books.aggregate( [
	 { $match: { authors : "Valerica Greavu-Serban" }},
        ] ) ;


//-- For displaying only the title for the books (co)written by "Valerica Greavu-Serban"...
db.books.find({"authors" : "Valerica Greavu-Serban" }, { "title" : 1, "_id" : 0 } ) ;

// ...we can use the AF query
db.books.aggregate( [
    { $match: { authors : "Valerica Greavu-Serban" }},
	  { $project : { _id : 0, title : 1 }}
        ] ) ;


//------------------------------------------------------------------------------
//--  Retrieve, in uppercase and alphabetical order, all the book titles
//------------------------------------------------------------------------------
db.books.aggregate( [
	{ $project : { title : {$toUpper : "$title"} , _id : 0 } },
	{ $sort : { title : 1 } } ] ) ;


//------------------------------------------------------------------------------
//--   Retrieve publishers, in uppercase and alphabetically ordered
//------------------------------------------------------------------------------
db.books.aggregate( [
	{ $project : { publisher : {$toUpper: "$publisher"} , _id:0 } },
	{ $sort : { publisher : 1 } } ] ) ;

// as "POLIROM" is repeated, we'll use "$group" as a DISTINCT operator:
db.books.aggregate( [
	{ $group : { _id: {$toUpper: "$publisher"} } },
	{ $project : { _id: 1 } },
	{ $sort : { _id : 1 } } ] ) ;


//------------------------------------------------------------------------------
//--             Display the sales amount for each book
//------------------------------------------------------------------------------

// SQL ~:  SELECT books.*, quantity_sold * price AS book_sales FROM books


// solution with $addFields
db.books.aggregate([
    { $addFields : { book_sales :
       { $multiply: [ "$quantity_sold", "$price" ] }
      } }
  ])


//------------------------------------------------------------------------------
//--                Extract the books published in 2014
//------------------------------------------------------------------------------

// sol. 1 - with `classical` filter
db.books.aggregate([
    { $match: { release_date : { $gte : ISODate("2014-01-01"), $lte : ISODate("2014-12-31")}}}
  ])

// sol. 2 - with $addFields
db.books.aggregate([
    { $addFields : { release_year : { $year : "$release_date" }} },
    { $match: { release_year : 2014}}
  ])




//===============================================================================
//--	     Selection (filter) and projection (extract/display) on arrays
//===============================================================================


//------------------------------------------------------------------------------
//--           Display the first three tags of each book
//------------------------------------------------------------------------------
db.books.aggregate([
  {$project : { title : 1, authors: 1, first_three_tags : { $slice : ["$tags", 3 ] }  }}
  ])


//------------------------------------------------------------------------------
//--             Display the books written by exacty two authors
//------------------------------------------------------------------------------

// solution with `$addFields` based on the size of array `authors`
db.books.aggregate([
    { $addFields : { n_of_authors : { $size : "$authors" }} },
    { $match: { n_of_authors : 2}}
  ])

// a shorter solution
db.books.aggregate([
    { $match: { authors : { $size : 2 }  }}
  ])


//------------------------------------------------------------------------------
//--           Display the books written by more than one author
//------------------------------------------------------------------------------

// next solution DOES NOT WORK ...
db.books.aggregate([
    { $match: { authors : { $size : { $gt : 1} }     }}
  ])

// ... but solution with ``$addFields` based on the size of array `authors` does:
db.books.aggregate([
    { $addFields : { n_of_authors : { $size : "$authors" }} },
    { $match: { n_of_authors : { $gt : 1}}}
  ])



//------------------------------------------------------------------------------
//--  Display the title, author and all topics (tags) for all the books
//       that cover the "Aggregation Framework" topic
//------------------------------------------------------------------------------

// a previous solution (adapted)
db.books.aggregate([
    { $project: { title : 1, authors: 1, tags : 1 }},
    { $match: { tags : "Aggregation Framework"}}
  ])


//------------------------------------------------------------------------------
//--  Display the title, author and ONLY the "Aggregation Framework" topic
//------------------------------------------------------------------------------

// solution based on `$filter`
db.books.aggregate([
    { $match: { tags : "Aggregation Framework"}},
    { $project: { title : 1, authors: 1,
        only_tag_af : {
          $filter: {
            input: "$tags",
            as: "tag",
            cond: { $eq: [ "$$tag", "Aggregation Framework" ] }
          }}}}
  ])


//------------------------------------------------------------------------------
//--  Display the title, author and ONLY the "SQL" or "NoSQL" topics
//------------------------------------------------------------------------------

// we need an `and` predicate in the array filter
db.books.aggregate([
    { $match: { tags : { $in : ["SQL", "NoSQL"] } }},
    { $project: { title : 1, authors: 1,
        sql_nosql__tags : {
          $filter: {
            input: "$tags",
            as: "tag",
            cond: { $or: [
                {$eq: [ "$$tag", "SQL" ] },
                {$eq: [ "$$tag", "NoSQL" ] }]}
          }}}}
  ])



//------------------------------------------------------------------------------
//--       Display only the comments who got at least five votes
//------------------------------------------------------------------------------

// solution with `$filter`
db.books.aggregate([
   { $project: { title : 1, authors : 1,
         comments: {
            $filter: {
               input: "$comments",
               as: "comment",
               cond: { $gte: [ "$$comment.votes", 5 ] }
            }
         }
      }
   }
])


//------------------------------------------------------------------------------
//--  Display the books (title and author) only if one the commenters
//--  is "Valerica Greavu-Serban" and his comment got exactly five votes
//------------------------------------------------------------------------------

// solution with ``$filter`
db.books.aggregate([
   {
      $project: { title : 1, authors : 1,
         comments: {
            $filter: {
               input: "$comments",
               as: "comment",
               cond: { $and: [
                 {$eq: [ "$$comment.user", "Valerica Greavu-Serban" ] },
                 {$eq: [ "$$comment.votes", 5 ] }]}
            }
         }
      }
   }
])


//------------------------------------------------------------------------------
//--  Display the books (title and author) only if one the commenters
//--  is "Valerica Greavu-Serban" and his comment got at least 3 votes
//------------------------------------------------------------------------------

// solution with ``$filter`
db.books.aggregate([
   {
      $project: { title : 1, authors : 1,
         comments: {
            $filter: {
               input: "$comments",
               as: "comment",
               cond: { $and: [
                 {$eq: [ "$$comment.user", "Valerica Greavu-Serban" ] },
                 {$gte: [ "$$comment.votes", 3 ] }]}
            }
         }
      }
   }
])



//===============================================================================
//--	               Agggregation with and without groups
//===============================================================================

//------------------------------------------------------------------------------
//--     How many documents are included in the "books" collection?
//------------------------------------------------------------------------------

// solution based on `$group`
db.books.aggregate( [
   { $group: { _id: null,
               n_of_books: { $sum : 1 }
             }
    }
     ])

//-- a simpler solution, based on $count
db.books.aggregate( [
   { $count: "title"}
		] ) ;


//------------------------------------------------------------------------------
//--                     Fing the average price of a book
//------------------------------------------------------------------------------

db.books.aggregate( [
   { $group: { _id: "all books",
               avg_price: { $avg : "$price" }
             }
    }
     ])



//------------------------------------------------------------------------------
//--                How many books were published by "Polirom"?
//------------------------------------------------------------------------------

// solution with `$group`
db.books.aggregate( [
  { $match: { publisher : "Polirom" }},
  { $group: { _id: "$publisher", n_of_books: { $sum: 1 } }}
  ] ) ;

// solution with `$count`
db.books.aggregate( [
  { $match: { publisher : "Polirom" }},
  { $count: "title" }
  ] ) ;



//------------------------------------------------------------------------------
//--        How many books were (co)written by "Valerica Greavu-Serban"?
//------------------------------------------------------------------------------

db.books.aggregate([
    { $match: { authors : "Valerica Greavu-Serban" }},
    { $group: { _id :  "author: Valerica Greavu-Serban" ,
               n_of_books: { $sum: 1 } }} ]) ;

//-- The same requirement, but remove author name from the result
db.books.aggregate([
    { $match: { authors : "Valerica Greavu-Serban" }},
    { $group: { _id: {"author: Valerica Greavu-Serban" : null},
               n_of_books: { $sum: 1 } }},
     {$project : { n_of_books:1,  _id: 0} }
     ]) ;

// solution with `$count`
db.books.aggregate( [
    { $match: { authors : "Valerica Greavu-Serban" }},
    { $count: "n_of_books" }
  ] ) ;


//------------------------------------------------------------------------------
//--                How many books have an associated URL?
//------------------------------------------------------------------------------

db.books.find( { url : {$exists : 1 } }) ;

db.books.aggregate( [
	 { $match: { url : {$exists : 1 } }},
   { $group: { _id: null,
               n_of_books: { $sum: 1 } }}
        ] ) ;

// we can name the group "_id", so that the result is more understandable
db.books.aggregate([
	 { $match: { url : {$exists : 1 } }},
   { $group: { _id: {n_of_books_with_associated_url: null},
               n_of_books: { $sum: 1 } }} ]) ;

// ... or even simpler
db.books.aggregate([
	 { $match: { url : {$exists : 1 } }},
   { $group: { _id: "n_of_books_with_associated_url", n_of_books: { $sum: 1 } } } ]) ;



//------------------------------------------------------------------------------
//--     Display for each book the total number of comments' votes
//------------------------------------------------------------------------------

// solution with $addFields based on the summing across array
db.books.aggregate([
    { $addFields : { n_of_comments_votes : { $sum : "$comments.votes" }} }
  ])


//------------------------------------------------------------------------------
//--     Display the books whose total number of comments' votes exceeds 13
//------------------------------------------------------------------------------

// solution with $addFields based on the summing across array
db.books.aggregate([
    { $addFields : { n_of_comments_votes : { $sum : "$comments.votes" }} },
    { $match: { n_of_comments_votes : { $gt : 13}}}
  ])


//------------------------------------------------------------------------------
//--                 Compute the total book sales
//------------------------------------------------------------------------------
db.books.aggregate([
    { $addFields : { book_sales : { $multiply: [ "$quantity_sold", "$price" ] } } },
    { $group: { _id: "total", book_sales: { $sum: "$book_sales" } } }
  ])



//===============================================================================
//--                             COUNT DISTINCT
//===============================================================================
// remember that in SQL...
// SELECT DISTINCT col1 FROM tab1
// ... is equivalent to...
// SELECT col1 FROM tab1 GROUP BY col1
//

//------------------------------------------------------------------------------
//--          How many publishers are there in the database/collection?
//------------------------------------------------------------------------------

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


//===============================================================================
//--                  Using $group for "regular" aggregation
//--		                       (like GROUP BY in SQL)
//===============================================================================


//------------------------------------------------------------------------------
//--              Display number of books for each publisher
//------------------------------------------------------------------------------
db.books.aggregate([
   { $group: { _id: {"publisher": "$publisher"},
            count: { $sum: 1 } } },
    { $sort : { _id : 1 } } ]) ;


//------------------------------------------------------------------------------
//--             Display average book prices for each publisher
//------------------------------------------------------------------------------
db.books.aggregate([
   { $group: { _id: {"publisher": "$publisher"},
            averagePrice: { $avg: "$price" } } },
    { $sort : { _id : 1 } } ]) ;



//------------------------------------------------------------------------------
//--  Display how many books cost less than 30 lei and how many more than 30
//------------------------------------------------------------------------------

db.books.aggregate([
   { $group: { _id: {$cond: [ { $lte: [ "$price", 30 ] }, 0, 1 ]},
               n_of_books: { $sum: 1 } }}
             ])

//... or, for a better look...
db.books.aggregate([
   { $group: { _id: { "Cost <= 30 RON (0=Yes, 1=No) " : {$cond: [ { $lte: [ "$price", 30 ] }, 0, 1 ]}},
               n_of_books: { $sum: 1 } }} ]) ;


//------------------------------------------------------------------------------
//--  Display how many books have an associated URL and how many haven't an URL
//------------------------------------------------------------------------------

/* see: http://stackoverflow.com/questions/25497150/mongodb-aggregate-by-field-exists  */
db.books.aggregate( [
   { $group: { _id: {$gt: ["$url", null]},
               n_of_books: { $sum: 1 } }} ]) ;

//... enhanced version:
db.books.aggregate( [
   { $group: { _id: {"With URL ": {$gt: ["$url", null]}},
               n_of_books: { $sum: 1 } }} ]) ;


//------------------------------------------------------------------------------
//--    Display the number of books covering "NoSQL" for each publisher
//------------------------------------------------------------------------------
db.books.aggregate([
   { $match: { tags : "NoSQL" } },
   { $group: { _id: { "publisher (of NoSQL books)": "$publisher"},
               n_of_books: { $sum: 1 } } },
    { $sort: { _id : 1 } }    ] )	;



//===============================================================================
//--                         Grouping by two attributes
//===============================================================================

//------------------------------------------------------------------------------
//--         Display number of books for each publsher and each year
/*  (SELECT publisher, EXTRACT(year FROM release_date) AS year, COUNT(*) AS n_of_books
    FROM books GROUP BY publisher, EXTRACT(year FROM release_date) )
    */
//------------------------------------------------------------------------------
db.books.aggregate([
   { $group: { _id: { publisher: "$publisher",
                      year: { "$year": "$release_date" }},
               n_of_books: { $sum: 1 } } },
    { $sort: { _id : 1 } }  ] ) ;


//===============================================================================
//--                       Group & filter (GROUP BY... HAVING...)
//===============================================================================

//------------------------------------------------------------------------------
//--   Display publishers with al least two published books covering "NoSQL"
//------------------------------------------------------------------------------
db.books.aggregate([
   	{ $match: { tags : "NoSQL" } },
   	{ $group: { _id: {"publisher": "$publisher"},
               n_of_books: { $sum: 1 } } },
    { $match: { n_of_books : { $gte : 2 } } },
    { $sort: { _id : 1 } }     ])	;


//===============================================================================
//--            Something with no equivalence in standard SQL:
//                  retrieve collections for each group
//===============================================================================

//------------------------------------------------------------------------------
//      Display all the book titles published by each publisher
//------------------------------------------------------------------------------
db.books.aggregate([
   { $group: { _id: "$publisher",
        "books: ": { $addToSet: "$title" } } },
   { $sort :{ _id: 1}}]) ;


//===============================================================================
//--	     Pivot tables "...a la Mongo": "$unwind" operator
//===============================================================================

//------------------------------------------------------------------------------
//--   Retrieve, in uppercase and alphabetically ordered, all the authors name
//------------------------------------------------------------------------------

// Next query generates an error, so we need "$unwind"
db.books.aggregate([
	{ $project : { name : {$toUpper:"$authors"} , _id:0 } },
	{ $sort : { name : 1 } }  ]) ;

/* "$unwind" deconstructs (unnest) an array field from the input documents and
    output a document for each array element.
    Each output document replaces the array with an element value.
    For each input document, the number of generated documents is the number of
        array elements (can be zero for an empty array).        */
// see what result produces next query
db.books.aggregate( [ { $unwind : "$authors" } ] )

//--    Now, returning to the required information:
// Retrieve, in uppercase and alphabetically ordered, all the authors name
db.books.aggregate([
   { $unwind: "$authors" } ,
   { $project : { author: {$toUpper: "$authors"}, _id: 0  }},
   { $sort: {author: 1 }}]) ;

// Next solution extracts no duplicates
db.books.aggregate([
    { $unwind: "$authors" } ,
    { $group : { _id: {$toUpper:"$authors"} } },
    { $project : { _id: 1 } },
    { $sort : { _id: 1 } } ]) ;



//------------------------------------------------------------------------------
//--            How many books were written by each author?
//------------------------------------------------------------------------------
db.books.aggregate([
    { $unwind: "$authors" } ,
    { $group : { _id: {"author": "$authors"},
               n_of_books: { $sum: 1 } }}]) ;


//------------------------------------------------------------------------------
//--                  Display the books written by each author?
//------------------------------------------------------------------------------
db.books.aggregate([
    { $unwind: "$authors" } ,
    { $group : { _id: {"author": "$authors"},
        "books: ": { $addToSet: "$title" } } },
    { $sort : { _id: 1 } } ]) ;


//------------------------------------------------------------------------------
//--         Display the number of books in which every tag is declared
//------------------------------------------------------------------------------
db.books.aggregate([
    { $unwind: "$tags" } ,
    { $group : { _id: {"tag": "$tags"},
               n_of_books: { $sum: 1 } }},
    { $project: {tag: 1, n_of_books: 1 } },
    { $sort :{ _id: 1}}]) ;


//------------------------------------------------------------------------------
//--             Display the books for which each tag is declared
//------------------------------------------------------------------------------
db.books.aggregate([
    { $unwind: "$tags" } ,
    { $group : { _id: {"tag": "$tags"},
        "books: ": { $addToSet: "$title" } } },
    { $sort : { _id: 1 } } ]) ;


//------------------------------------------------------------------------------
//--                   Find the overall number of votes
// as "votes" is an attribute of "comments" (which is an array);
//  "comments" must be unwinded
//------------------------------------------------------------------------------
db.books.aggregate([
   { $unwind: "$comments" } ,
   { $group: { _id: null, "votes_overall": { $sum: "$comments.votes" } } }
 ] ) ;


//------------------------------------------------------------------------------
//--    Display the number of received votes by the comments of each book
//------------------------------------------------------------------------------
db.books.aggregate([
   { $unwind: "$comments" } ,
   { $group: { _id: "$title",
        "n_of_votes": { $sum: "$comments.votes" } } },
   { $sort :{ _id: 1}}]) ;


//------------------------------------------------------------------------------
//--               Number of posts written by each commenter (user)
//------------------------------------------------------------------------------
db.books.aggregate([
   { $unwind: "$comments" },
   { $group: { _id: {"user": "$comments.user"},
         n_of_posts : { $sum: 1 } } },
    { $sort: {user: 1 } } ]) ;


//------------------------------------------------------------------------------
//--              Display commenters with at least three posts
//------------------------------------------------------------------------------

db.books.aggregate([
   { $unwind: "$comments" },
   { $group: { _id: {"user": "$comments.user"},
         n_of_posts : { $sum: 1 } } },
    { $match: {n_of_posts: {$gte :3}}},
    { $sort: {n_of_posts: -1 } ] } ) ;


//------------------------------------------------------------------------------
//--    Display top 3 of commenters (in terms of number of posts)
//------------------------------------------------------------------------------

db.books.aggregate([
   { $unwind: "$comments" },
   { $group: { _id: {"user": "$comments.user"},
         n_of_posts : { $sum: 1 } } },
   { $sort: {n_of_posts:  -1} },
   { $limit: 3}] ) ;



//===============================================================================
//              		   Other Aggregation Framework features
//===============================================================================

//------------------------------------------------------------------------------
//        Pass the result of an "unwind" operator to another "unwind"
//------------------------------------------------------------------------------

db.books.aggregate([
   { $unwind: "$authors" },
   { $unwind: "$comments" }
 ])


//------------------------------------------------------------------------------
//--    Display for each author the number of comments and the sum of votes for comments
//  posted for his/her books
//------------------------------------------------------------------------------

db.books.aggregate([
   { $unwind: "$authors" },
   { $unwind: "$comments" },
   { $group: { _id: { author: "$authors"},
        n_of_posts: { $sum: 1 } ,
        sum_of_votes : { $sum: "$comments.votes" } } },
    { $sort: {_id: 1} }]) ;



//------------------------------------------------------------------------------
//--    Display the books for which at least one of the authors appears as
//--              one of book's commenters
//------------------------------------------------------------------------------

// next solution DOES NOT WORK! ...
db.books.aggregate([
   { $match: { comments : {$exists : 1 } }},
   { $project : {title : 1, authors : 1, comments : 1} },
   { $unwind: "$authors" },
   { $unwind: "$comments" },
   { $match: { "$authors" : "$comments.user" }}
])

// but the next one does; notice `$expr` included in `$match`
db.books.aggregate([
   { $match: { comments : {$exists : 1 } }},
   { $project : {title : 1, authors : 1, comments : 1} },
   { $unwind: "$authors" },
   { $unwind: "$comments" },
   { $match: {  $expr : { $eq : [ "$authors", "$comments.user"  ]  } }}
])


// a more optimized solution -  notice `$ROOT`
db.books.aggregate([
   { $match: { comments : {$exists : 1 } }},
   { $project : {title : 1, authors : 1, comments : 1} },
   { $unwind: "$authors" },
   { $project: { title : 1, authors: 1,
        commenter_and_author : {
          $filter: {
            input: "$comments",
            as: "comment",
            cond: { $eq: [ "$$comment.user", "$$ROOT.authors" ] }
          }}}},
    { $addFields : { size_commenter_and_author : { $size : "$commenter_and_author" }} },
    { $match: { size_commenter_and_author : { $gt : 0}}}
])


// change the structure of the resul with `$replaceRoot`
db.books.aggregate([
   { $match: { comments : {$exists : 1 } }},
   { $project : {title : 1, authors : 1, comments : 1} },
   { $unwind: "$authors" },
   { $unwind: "$comments" },
   { $addFields : { comments_and_authors : {
        title : "$title", new_author : "$authors", new_user : "$comments.user" }} },
    { $replaceRoot: { newRoot: "$comments_and_authors" } }

 ])


//------------------------------------------------------------------------------
/* for additional (and some other advanced) examples of using Aggregation Framework
  ( "$out" and "$lookup" operators, different functions for dealing with
  numbers, strings ans dates), and also examples that deal with  "normalized"
  collections - see script `01-05b_MongoDB - Case study - Sales_2_queries.js`
*/
//------------------------------------------------------------------------------
