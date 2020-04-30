//###################################################################################
//### 													     Movies
//###                This is a Neo4j Desktop built-in dataset
//###################################################################################

//###################################################################################
//###								       Aggregate queries without grouping


//# 	How many people are in the database?
MATCH (p:Person)
RETURN COUNT(*) AS n_of_people


//# 	How many actors are in the database (people casted in at least one movie) ?

// this is a wrong solution:
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
RETURN COUNT(p) AS n_of_people

// the proper solution uses DISTINCT:
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
RETURN COUNT(DISTINCT p) AS n_of_people


//# 	Display the birth year of the oldest people in the database?
MATCH (p:Person)
RETURN MIN(p.born)


//# 	Display the oldest people in the database?

// solution without aggregation (correct, but incomplete)
MATCH (p:Person)
RETURN p.name, p.born
ORDER BY p.born
LIMIT 1


// first solution
MATCH (p:Person)
WITH MIN(p.born) AS first_year
MATCH (p:Person {born: first_year})
RETURN p

// second solution
MATCH (p:Person)
WITH MIN(p.born) AS first_year
MATCH (p2:Person)
WHERE p2.born IN first_year
RETURN p2



//# 	Display the oldest and the youngest people in the database?

// first solution
MATCH (p:Person)
WITH MIN(p.born) AS first_year, MAX(p.born) AS last_year
MATCH (p2:Person)
WHERE p2.born IN first_year OR p2.born IN last_year
RETURN p2.name, p2.born

// second solution
MATCH (p:Person)
WITH MIN(p.born) AS first_year, MAX(p.born) AS last_year
WITH first_year, last_year
MATCH (p:Person)
WHERE p.born IN [first_year, last_year]
RETURN p.name, p.born



//# 	Display the people born in the first two years (of the years of
//   birth for the people in the database)?
MATCH (p:Person)
WITH MIN(p.born) AS first_year
MATCH (p2:Person)
WHERE p2.born > first_year
WITH MIN(p2.born) AS second_year
MATCH (p3:Person)
WHERE p3.born <= second_year
RETURN p3


//# 	Display the people born in the first 10 years (of the years of
//   birth for the people in the database)?
MATCH (p1:Person)
WITH p1.born AS years
ORDER BY p1.born
LIMIT 10
WITH years
MATCH (p2:Person)
WHERE p2.born IN years
RETURN p2




//###################################################################################
//###								          Aggregation with groups


//# 	Display the number of actors casted in every movie released in 2003?
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE m.released = 2003
RETURN m.title, COUNT(p) AS n_of_actors
ORDER BY m.title

//# 	Display the number movies for each actor
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
RETURN p.name, COUNT(m) AS n_of_movies
ORDER BY p.name


//# 	Display the number movies for each person (actors, directors, writers, producers)
MATCH (p:Person) -[]-> (m:Movie)
RETURN p.name, COUNT(m) AS n_of_movies
ORDER BY p.name


//# 	Display the number movies for each person and position (actor, director, ...)
MATCH (p:Person) -[r]-> (m:Movie)
RETURN p.name, type(r), COUNT(m) AS n_of_movies
ORDER BY p.name, type(r)


//# 	Display the people who directed at least two movies
MATCH (p:Person) -[r:DIRECTED]-> (m:Movie)
WITH p, COUNT(m) AS n_of_movies
WHERE n_of_movies >=2
WITH p, n_of_movies
RETURN p.name, n_of_movies
ORDER BY n_of_movies DESC



//###################################################################################
//###				LEFT JOIN (reprise), COALESCE, CASE, UNION


//# 	Display all people and the the number of movies she/he directed
// (the report will include people with no directed movies)

// first (raw) solution
MATCH (p:Person)
OPTIONAL MATCH (p) -[r:DIRECTED]-> (m:Movie)
RETURN p.name, type(r), COUNT(m) AS n_of_directed_movies
ORDER BY p.name, type(r)


// solution that uses COALESCE for a better form of the results
MATCH (p:Person)
OPTIONAL MATCH (p) -[r:DIRECTED]-> (m:Movie)
RETURN p.name, COALESCE(type(r), 'no director') AS is_she_director,
  COUNT(m) AS n_of_directed_movies
ORDER BY p.name


//# 	Display numbers of people who acted as actors, directors AND
//  other positions/roles for making the movies

// solution with CASE
MATCH (p:Person) -[r]-> (m:Movie)
RETURN
    CASE
    WHEN type(r) = 'DIRECTED' THEN 'directors'
    WHEN type(r) = 'ACTED_IN' THEN 'actors'
    ELSE 'other positions'
    END AS movie_positions,
    COUNT(p) AS n_of_people
ORDER BY  movie_positions


//...and a solution based on UNION
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
RETURN 'actors' AS movie_positions, COUNT(p) AS n_of_people
UNION
MATCH (p:Person) -[r:DIRECTED]-> (m:Movie)
RETURN 'directors' AS movie_positions, COUNT(p) AS n_of_people
UNION
MATCH (p:Person) -[r]-> (m:Movie)
WHERE NOT type(r) IN ['ACTED_IN', 'DIRECTED']
RETURN 'other positions' AS movie_positions, COUNT(p) AS n_of_people
