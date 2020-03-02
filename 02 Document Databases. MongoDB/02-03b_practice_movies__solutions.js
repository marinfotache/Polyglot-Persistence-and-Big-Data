/* ---------------------------------------------------------------*/
/* Create the `movies` collection in a MongoDB database with the
    same name, based of the file  `movies_2011-2018.json`
The file is available at:
https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/02%20Document%20Databases.%20MongoDB/movies_2011-2018.json
/* ---------------------------------------------------------------*/



/* ---------------------------------------------------------------*/
/* Task 1: Provide to following information using `find` command */
/* ---------------------------------------------------------------*/

/* extract all documents */

/* extract movies released in 2018 */

/* extract movies released between 2015 and 2018 */

/* extract movies released after 2015 */

/* extract all movies whose genre is `Comedy` */

/* extract all movies whose genre is `Comedy` or `Romance` */

/* extract all movies whose genre is (at least) both `Comedy` and `Romance` */

/* extract all movies  with exactly five main actors in the cast */

/* extract first three main actors for each movie*/



/* ---------------------------------------------------------------*/
/* Task 2: Provide to following information using `find` command */
/* ---------------------------------------------------------------*/

/* Add two properties in each document of collection `movies`
    - n_of_genres
    - n_of_main_actors
*/    
    








/* ---------------------------------------------------------------*/
/* Task 1: Provide to following information using `find` command */
/* ---------------------------------------------------------------*/

/* extract all documents */
db.movies.find()


/* extract movies released in 2018 */
db.movies.find({"year" : 2018})


/* extract movies released between 2015 and 2018 */


/* extract movies released after 2015 */


/* extract all movies whose genre is `Comedy` */
db.movies.find({'genres': 'Comedy'});


/* extract all movies whose genre is `Comedy` or `Romance` */

/* sol. 1 */
db.movies.find({'genres': 'Comedy', 'genres': 'Romance'});


/* sol. 2 - "$in" operator*/
db.movies.find({'genres':  { "$in" : ["Comedy", "Romance" ]}});


/* extract all movies whose genre is (at least) both `Comedy` and `Romance` */

/* sol. 1 - $and */
db.movies.find( { $and: [ {"genres" : "Comedy"}, {"genres" : "Romance" }] } ) ;

/* sol. 2 - $all */
db.movies.find( {"genres" : { $all : ["Comedy", "Romance" ] }} ) ;



/* extract all movies  with exactly five main actors in the cast */
db.movies.find({"cast" : {"$size" : 5}}) ;


/* extract all movies  with more than five main actors in the cast */
/* DOES NOT WORK !!!*/
db.movies.find({"cast" : {"$size" : {"$gte" : 5}}}) ;


/* extract first three main actors for each movie*/
db.movies.find({}, {cast : {$slice : -3}}) ;


/* ---------------------------------------------------------------*/
/* Task 2:
    Add two properties in each document of collection `movies`
    - n_of_genres
    - n_of_main_actors                                            */    
/* ---------------------------------------------------------------*/

var myCursor = db.movies.find( {}) ;
myCursor.forEach(function(x) {
        x.n_of_genres = x.genres.length ;
        x.n_of_main_actors = x.cast.length ;    
	// print(x.n_of_genres) ;
        db.movies.update({_id : x._id}, x) ;
	} ) ;

