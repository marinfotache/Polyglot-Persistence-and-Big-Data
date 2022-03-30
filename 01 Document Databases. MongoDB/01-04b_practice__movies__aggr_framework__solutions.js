// last update: 2022-02-21

/* ---------------------------------------------------------------*/
/* Create the `movies` collection in a MongoDB database with the
    same name, based of the file  `movies_2011-2018.json`
The file is available at:
https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/01%20Document%20Databases.%20MongoDB/movies_2011-2018.json/* ---------------------------------------------------------------*/



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
	{ $match: { "year" : 2018}}
        ] ) ;


/* ---------------------------------------------------------------*/
/*          extract movies released between 2015 and 2018      */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
	{ $match: { $and : [ {"year" : { $gte : 2015} },
                    {"year" : { $lte : 2018} }] }}
        ] ) ;


/* ---------------------------------------------------------------*/
/* extract all movies with exactly five main actors in the cast */
/* ---------------------------------------------------------------*/
// $size
db.movies.aggregate([
    { $addFields : { n_of_actors : { $size : "$cast" }} },
    { $match: { n_of_actors : 5}}
  ])



/* ---------------------------------------------------------------*/
/*       extract first three main actors for each movie           */
/* ---------------------------------------------------------------*/
// $slice
db.movies.aggregate([
   { $project: { title: 1, first_3actors : { $slice: [ "$cast", 3 ] } } }
])


/* ---------------------------------------------------------------*/
/*    for the movies with at least five actors, display
          only the first three (main actors)                       */
/* ---------------------------------------------------------------*/
db.movies.aggregate([
    { $addFields : { n_of_actors : { $size : "$cast" }} },
    { $match: { n_of_actors : { $gte : 5}}},
    { $project : { title : 1, year: 1, genres : 1,
        first_3actors_of5 : { $slice : ["$cast", 3 ] }  }}
  ])



/* ---------------------------------------------------------------*/
/*            extract all movies whose genre is `Comedy`          */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
	{ $match: { 'genres': 'Comedy' }}
        ] ) ;


/* ---------------------------------------------------------------*/
/*   extract all movies whose genre is `Comedy`; display only
              this genre for each extracted movie */
/* ---------------------------------------------------------------*/

db.movies.aggregate( [
	{ $match: { 'genres': 'Comedy' }},
  { $project: { title : 1,
        only_comedy : {
          $filter: {
            input: "$genres",
            as: "genre",
            cond: { $eq: [ "$$genre", "Comedy" ] }
          }}}}
  ])



/* ---------------------------------------------------------------*/
/* extract all movies whose genre is `Comedy` or `Romance` */
// hint: use array filtering
/* ---------------------------------------------------------------*/

// sol. without array filtering
db.movies.aggregate( [
	{ $match: { 'genres':  { "$in" : ["Comedy", "Romance" ]} }}
        ] ) ;


// sol. with array filtering
db.movies.aggregate( [
	{ $match: { 'genres':  { "$in" : ["Comedy", "Romance" ]} }},
  { $project: { title : 1,
        only_comedy : {
          $filter: {
            input: "$genres",
            as: "genre",
            cond: { $or: [
                {$eq: [ "$$genre", "Comedy" ] },
                {$eq: [ "$$genre", "Romance" ] }]}
          }}}}
  ])



/* ---------------------------------------------------------------*/
/* extract all movies whose genre is (at least) both `Comedy`
                    and `Romance` */
// hint: use array filtering
/* ---------------------------------------------------------------*/

/* extract all movies whose genre is (at least) both `Comedy` and `Romance` */

/* sol. 1 - $and */
db.movies.aggregate( [
	{ $match: { $and : [ {"genres" : "Comedy" }, {"genres" : "Romance"}] } }
        ] ) ;


/* sol. 2 - $all */
db.movies.aggregate( [
	{ $match: { genres :  { $all : [ "Comedy", "Romance"] } } }
        ] ) ;




/* ---------------------------------------------------------------*/
/*        Display the number of movies released in each year     */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
    { $group: { _id: "$year", n_of_movies : { $sum : 1} } },
    { $sort : { _id : 1 } }
    ])


/* ---------------------------------------------------------------*/
/*   Extract the year with the largest number of released movies  */
/* ---------------------------------------------------------------*/
db.movies.aggregate( [
    { $group: { _id: "$year", n_of_movies : { $sum : 1} } },
    { $sort : { n_of_movies : -1 } },
		{ $limit : 1}
    ])







/* ---------------------------------------------------------------*/
/* extract all actors playing in the movies, in alphabetical order */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*            extract the number of movies for each actor          */
/* ---------------------------------------------------------------*/

db.movies.aggregate( [
	{ $unwind : "$cast" },
        { $group: { _id: "$cast", n_of_movies : { $sum : 1} } },
        { $sort : { _id : 1 } }
        ])

/* ---------------------------------------------------------------*/
/*            extract the most popular movie genre                */
/* ---------------------------------------------------------------*/



/* ---------------------------------------------------------------*/
/*            extract top 5 actors that have worked in the        */
/*            largest number of movies with Anna Kendrick            */
/* ---------------------------------------------------------------*/
//
db.movies.aggregate( [
	{ $match: { 'cast' : 'Anna Kendrick' } },
	{ $unwind : '$cast' },
  { $group: { _id: "$cast", n_of_movies_with_th : { $sum : 1} } },
  { $sort : { n_of_movies_with_th : -1 } },
	{ $limit : 6 }  // Anna Kendrick is included in the result
        ])


db.movies.aggregate( [
	{ $match: { 'cast' : 'Anna Kendrick' } },
	{ $unwind : '$cast' },
	{ $match: { 'cast' : { $ne : 'Anna Kendrick' } }}, // Anna Kendrick is NOT included in the result
  { $group: { _id: "$cast", n_of_movies_with_th : { $sum : 1} } },
  { $sort : { n_of_movies_with_th : -1 } },
	{ $limit : 5 }
        ])




/*  extract the number of movies for each actor whose name starts
        with `B`, `C`, ... `Z`*/
db.movies.aggregate( [
	{ $unwind : "$cast" },
        { $group: { _id: "$cast", n_of_movies : { $sum : 1} } },
        { $sort : { _id : 1 } },
        { $match : {_id : {$gte : 'B'} }}
        ])



/* extract all movies  with exactly five main actors in the cast */


// sol 2
db.movies.aggregate( [
	{ $project: { title : 1, n_of_actors : { $size : "$cast"}  } } ,
	{ $match: { n_of_actors : 5 } }
        ] ) ;

// sol. 3
db.movies.aggregate( [
	{ $unwind : "$cast" },
        { $group :{ _id: "$title", n_of_actors :  { $sum : 1 } } },
        { $match :{ n_of_actors :  5 } },
	{ $sort : { _id : 1 } }
        ] )


/* extract all movies  with more than five main actors in the cast */

// sol 1
db.movies.aggregate( [
	{ $project: { title : 1, n_of_actors : { $size : "$cast"}  } } ,
	{ $match: { n_of_actors : { $gte : 5 }} },
	{ $sort : { n_of_actors : -1 } }
        ] ) ;


// sol 2
db.movies.aggregate( [
	{ $unwind : "$cast" },
        { $group :{ _id: "$title", n_of_actors :  { $sum : 1 } } },
        { $match :{ n_of_actors : { $gte : 5 } } },
	{ $sort : { n_of_actors : -1 } }
        ] )



// display the most popular movie genre







/* extract first three main actors for each movie*/

db.movies.aggregate([
   { $project: { title: 1, three_actors: { $slice: [ "$cast", 3 ] } } }
])
