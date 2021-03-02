
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


/* ---------------------------------------------------------------*/
/*          extract movies released between 2015 and 2018         */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/* extract all movies with exactly five main actors in the cast */
/* ---------------------------------------------------------------*/
// $size


/* ---------------------------------------------------------------*/
/*       extract first three main actors for each movie           */
/* ---------------------------------------------------------------*/
// $slice


/* ---------------------------------------------------------------*/
/*    for the movies with at least five actors, display
          only the first three (main actors)                       */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*            extract all movies whose genre is `Comedy`          */
/* ---------------------------------------------------------------*/


/* ---------------------------------------------------------------*/
/*   extract all movies whose genre is `Comedy`; display only
              this genre for each extracted movie */
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
//

/* ---------------------------------------------------------------*/
/*            extract the most popular movie genre                */
/* ---------------------------------------------------------------*/
//


/* ---------------------------------------------------------------*/
/*            extract top 5 actors that have worked in the        */
/*          largest number of movies with Anna Kendrick           */
/* ---------------------------------------------------------------*/
//


/*  extract the number of movies for each actor whose name starts
        with `B`...`Z`*/
