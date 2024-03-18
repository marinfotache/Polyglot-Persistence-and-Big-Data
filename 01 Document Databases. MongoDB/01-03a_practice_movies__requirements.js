// last update: 2022-02-21
/* ---------------------------------------------------------------*/
/* Create the `movies` collection in a MongoDB database with the
    same name, based of the file  `movies_2011-2018.json`
The file is available at:
https://github.com/marinfotache/Polyglot-Persistence-and-Big-Data/blob/master/01%20Document%20Databases.%20MongoDB/movies_2011-2018.json

Create a database called `movies`

Make `movies` your default database
use movies

Include the .json file as the argument for:

db.movies.insertMany(
........................paste here the content of `movies_2011-2018.json` file
)


/* ---------------------------------------------------------------*/
// last update: 2022-02-21



/* ---------------------------------------------------------------*/
/* Task 1: Provide to following information using `find` command */
/* ---------------------------------------------------------------*/

/* extract all documents */

/* extract movies released in 2018 */
db.movies.aggregate([
    { $match: { year : 2018}}
  ])


/* extract movies released between 2015 and 2018 */


/* extract movies released after 2015 */




/* extract all movies whose genre is `Comedy` */

/* extract all movies whose genre is `Comedy` or `Romance` */


/* extract all movies whose genre is (at least) both `Comedy` and `Romance` */
db.movies.aggregate([
    {$match :  { "genres" : { $all : ["Comedy", "Romance" ] }} }
    ])
 
 
 



/* extract all movies with exactly five main actors in the cast */

/* extract first three main actors for each movie*/



/* ---------------------------------------------------------------*/
/* Task 2:
    Add two properties in each document of collection `movies`
    - n_of_genres
    - n_of_main_actors                                            */
/* ---------------------------------------------------------------*/
