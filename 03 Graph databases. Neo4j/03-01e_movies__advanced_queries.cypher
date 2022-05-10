//###################################################################################
//### 													     Movies
//###                This is a Neo4j Desktop built-in dataset
//###################################################################################
//### last update: 2022-04-19


//###################################################################################
//###								          (Slightly) More advanced queries
//###################################################################################


//# Display the actor(s) playing in the largest number of movies

// ... an incomplete solution (in case there are two or more actors with the
//      same (maximum) number of movies)
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WITH p, COUNT(m) AS n_of_movies
ORDER BY n_of_movies DESC
LIMIT 1
RETURN p.name, n_of_movies


// first solution - with LIMIT
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WITH p, COUNT(m) AS n_of_movies
ORDER BY n_of_movies DESC
LIMIT 1
WITH n_of_movies AS max_n_of_movies
MATCH (p2:Person) -[r2:ACTED_IN]-> (m2:Movie)
WITH max_n_of_movies, p2, COUNT(m2) AS n_of_movies2
WHERE n_of_movies2 = max_n_of_movies
RETURN p2.name, n_of_movies2


// second solution - with collections
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WITH p, COUNT(m) AS n_of_movies
ORDER BY n_of_movies DESC
WITH COLLECT(n_of_movies) AS coll_n_of_movies
WITH coll_n_of_movies[0] AS max_n_of_movies
MATCH (p2:Person) -[r2:ACTED_IN]-> (m2:Movie)
WITH max_n_of_movies, p2, COUNT(m2) AS n_of_movies2
WHERE n_of_movies2 = max_n_of_movies
RETURN p2.name, n_of_movies2


//# Display the actor(s) playing in the largest number of characters

// sol. 1 - LIMIT
MATCH (p:Person) -[r:ACTED_IN]-> ()
WITH p,  r UNWIND r.roles AS movie_role
WITH p, COUNT(movie_role) AS n_of_roles
ORDER BY n_of_roles DESC
LIMIT 1
WITH n_of_roles AS max_n_of_roles
MATCH (p2:Person) -[r2:ACTED_IN]-> ()
WITH max_n_of_roles, p2,  r2 UNWIND r2.roles AS movie_role2
WITH max_n_of_roles, p2, COUNT(movie_role2) AS n_of_roles2
WHERE n_of_roles2 = max_n_of_roles
RETURN p2.name, n_of_roles2

// sol. 2 - with collections
MATCH (p:Person) -[r:ACTED_IN]-> ()
WITH p,  r UNWIND r.roles AS movie_role
WITH p, COUNT(movie_role) AS n_of_roles
ORDER BY n_of_roles DESC
WITH COLLECT(n_of_roles) AS coll_n_of_roles
WITH coll_n_of_roles[0] AS max_n_of_roles
MATCH (p2:Person) -[r2:ACTED_IN]-> ()
WITH max_n_of_roles, p2,  r2 UNWIND r2.roles AS movie_role2
WITH max_n_of_roles, p2, COUNT(movie_role2) AS n_of_roles2
WHERE n_of_roles2 = max_n_of_roles
RETURN p2.name, n_of_roles2



//# Display top 5 actors playing in the largest number of movies
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WITH p, COUNT(m) AS n_of_movies
ORDER BY n_of_movies DESC
LIMIT 5
WITH COLLECT(n_of_movies) AS top5_n_of_movies
MATCH (p2:Person) -[r2:ACTED_IN]-> (m2:Movie)
WITH top5_n_of_movies, p2, COUNT(m2) AS n_of_movies2
WHERE n_of_movies2 IN top5_n_of_movies
RETURN p2.name, n_of_movies2
ORDER BY n_of_movies2



//# Display top 5 actors playing in the largest number of characters
MATCH (p:Person) -[r:ACTED_IN]-> ()
WITH p,  r UNWIND r.roles AS movie_role
WITH p, COUNT(movie_role) AS n_of_roles
ORDER BY n_of_roles DESC
LIMIT 5
WITH COLLECT(n_of_roles) AS top5_n_of_roles
MATCH (p2:Person) -[r2:ACTED_IN]-> ()
WITH top5_n_of_roles, p2,  r2 UNWIND r2.roles AS movie_role2
WITH top5_n_of_roles, p2, COUNT(movie_role2) AS n_of_roles2
WHERE n_of_roles2 IN top5_n_of_roles
RETURN p2.name, n_of_roles2
ORDER BY n_of_roles2 DESC



//# Display the actor(s) playing in a larger number of movies than `Keanu Reeves`

MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE p.name = 'Keanu Reeves'
WITH COUNT(m) AS n_of_movies_keanu
WITH n_of_movies_keanu
MATCH (p2:Person) -[r2:ACTED_IN]-> (m2:Movie)
WITH p2, COUNT(m2) AS n_of_movies2, n_of_movies_keanu
WHERE n_of_movies2 > n_of_movies_keanu
RETURN p2.name, n_of_movies2, n_of_movies_keanu



//# Display the actor(s) playing in at least one of "Carrie-Anne Moss" movies

// solution proposed in script `04-01b`...
MATCH (p:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (carrie:Person)
WHERE carrie.name = 'Carrie-Anne Moss'
RETURN p.name, m.title, carrie.name


// a new solution based on collections
MATCH (m:Movie) <-[r2:ACTED_IN]- (carrie:Person)
WHERE carrie.name = 'Carrie-Anne Moss'
WITH COLLECT (m.title) AS movies_carrie
MATCH (p:Person) -[r:ACTED_IN]-> (m2:Movie)
WHERE m2.title IN movies_carrie
RETURN p.name, m2.title


// another solution based on collections
MATCH (m:Movie) <-[:ACTED_IN]- (carrie:Person)
WHERE carrie.name = 'Carrie-Anne Moss'
WITH COLLECT (m.title) AS movies_carrie
MATCH (p:Person) -[:ACTED_IN]-> (m2:Movie)
WITH p.name AS name, COLLECT(m2.title) AS movies_actor, movies_carrie
WHERE ANY (movie IN movies_actor WHERE movie IN movies_carrie)
RETURN name, movies_actor, movies_carrie



//# Display the actor(s) playing in all of "Carrie-Anne Moss" movies

// solution based on collections

MATCH (m:Movie) <-[:ACTED_IN]- (carrie:Person)
WHERE carrie.name = 'Carrie-Anne Moss'
WITH COLLECT (m.title) AS movies_carrie
MATCH (m) <-[:ACTED_IN]- (p:Person)
WHERE m.title IN movies_carrie
WITH p.name AS name, COLLECT(m.title) AS movies_carrie_actor, movies_carrie
WHERE SIZE(movies_carrie_actor) = SIZE(movies_carrie)
RETURN name, movies_carrie_actor, movies_carrie


// another solution based on collections
MATCH (m:Movie) <-[:ACTED_IN]- (carrie:Person)
WHERE carrie.name = 'Carrie-Anne Moss'
WITH COLLECT (m.title) AS movies_carrie
MATCH (p:Person) -[:ACTED_IN]-> (m2:Movie)
WHERE m2.title IN movies_carrie
WITH p.name AS name, COLLECT(m2.title) AS movies_actor, movies_carrie
WHERE ALL (movie IN movies_carrie  WHERE movie IN movies_actor)
RETURN name, movies_actor, movies_carrie
