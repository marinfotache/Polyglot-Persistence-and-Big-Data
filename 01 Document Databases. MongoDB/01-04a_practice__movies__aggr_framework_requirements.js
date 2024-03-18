// last update: 2022-03-07

/* ---------------------------------------------------------------*/
/* Create (if not already created) the `movies` collection in a
  MongoDB database with the same name, based of the file
  `movies_2011-2018.json`
The file is available at:
https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/01%20Document%20Databases.%20MongoDB/movies_2011-2018.json
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/* Extract from the `movies` collection the following information
    using Aggregation Framework
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*              extract movies released in 2018                   */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
	 { $match: { year : 2018 }}
        ] ) ;



/* ---------------------------------------------------------------*/
/*          extract movies released between 2015 and 2018         */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
	 { $match: { year : { $gte : 2015, $lte : 2018} }}
        ] ) ;


/* ---------------------------------------------------------------*/
/* extract all movies with exactly five main actors in the cast */
/* ---------------------------------------------------------------*/

// solution based on $addFields and $match
db.movies.aggregate( [
    { $addFields : { n_of_actors : { $size : "$cast" }} },
    { $match: { n_of_actors : 5}}
  ])


// solution based on $unwind 
db.movies.aggregate( [
   { $unwind: "$cast" },
   { $group: { _id: {"movie_title": "$title"}, n_of_actors: { $sum: 1 } } },
   { $match : {n_of_actors : 5} }, 
   { $project : { title : "$_id.movie_title", n_of_actors : 1, _id : 0 } }
 
  ])



// but the next one does; notice `$expr` included in `$match`
db.books.aggregate([
   { $match: { comments : {$exists : 1 } }},
   { $project : {title : 1, authors : 1, comments : 1} },
   { $unwind: "$authors" },
   { $unwind: "$comments" },
   { $match: {  $expr : { $eq : [ "$authors", "$comments.user"  ]  } }}
])




/* ---------------------------------------------------------------*/
/* extract all movies with more than five main actors in the cast */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
    { $addFields : { n_of_actors : { $size : "$cast" }} },
    { $match: { n_of_actors : { $gt : 5} }}
  ])





/* ---------------------------------------------------------------*/
/*       extract first three main actors for each movie           */
/* ---------------------------------------------------------------*/

//  { $limit: 3}] ) ;

/* ---------------------------------------------------------------*/
/*    for the movies with at least five actors, display
          only the first three (main actors)                       */
/* ---------------------------------------------------------------*/




/* ---------------------------------------------------------------*/
/*            extract all movies whose genre is `Comedy`          */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*   extract all movies whose one of their genre is `Comedy`;     */
/*   display only this genre for each extracted movie             */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/* extract all movies whose genre is `Comedy` or `Romance` */
// hint: use array filtering
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/* extract all movies whose genre is (at least) both `Comedy`
                    and `Romance` */
// hint: use array filtering
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*        Display the number of movies released on each year     */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*  Extract the year with the largest number of released movies  */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*            extract the number of movies for each actor          */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
   { $unwind: "$cast" },
   { $group: { _id: {"actor_name": "$cast"}, n_of_movies: { $sum: 1 } } },
   { $project : { _id : 0, actor : "$_id.actor_name", n_of_movies : 1  } } 
   ]) ;


/* ---------------------------------------------------------------*/
/*            extract the most popular movie genre                */
/* ---------------------------------------------------------------*/
// (c) Straton
db.movies.aggregate([
    { $unwind: "$genres" } ,
    { $group: { _id: {"genuri": "$genres"}, n_of_movies: {$sum : 1}}},    
    { $sort : { n_of_movies : -1 } },
    { $limit : 1}, 
    { $project : {"genu'" : "$_id.genuri", "numar de filme" : "$n_of_movies", _id : 0 } }
    ]); 


/* ---------------------------------------------------------------*/
/*            extract top 5 actors that have worked in the        */
/*          largest number of movies with Anna Kendrick           */
/* ---------------------------------------------------------------*/
//


/*  extract the number of movies for each actor whose name starts
        with `B`...`Z`*/
