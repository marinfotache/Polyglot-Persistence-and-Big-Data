//###################################################################################
//### 													     Movies
//###                This is a Neo4j Desktop built-in dataset
//###################################################################################
//### last update: 2022-04-19


//###################################################################################
//###							           Queries with collections
//###################################################################################


//# 	Display the all roles Tom Hanks played in the 'The Polar Express' movie
MATCH (th:Person) -[r:ACTED_IN]-> (pe:Movie)
WHERE th.name = 'Tom Hanks' AND pe.title = 'The Polar Express'
RETURN r.roles


//# 	How many roles Tom Hanks played in the 'The Polar Express' movie?
MATCH (th:Person) -[r:ACTED_IN]-> (pe:Movie)
WHERE th.name = 'Tom Hanks' AND pe.title = 'The Polar Express'
RETURN SIZE(r.roles) AS n_of_roles


//# 	Extract all actors and movies in which the actor played at least two roles
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE SIZE(r.roles) >= 2
RETURN p.name, r.roles, m.title, m.released


//# 	Extract only the first of the roles Tom Hanks played in each of his movies
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE p.name = 'Tom Hanks' AND SIZE(r.roles) >= 2
RETURN p.name, HEAD(r.roles), m.title, m.released


//# 	Display, for each actor, all characters she/he played in her/his movies
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
RETURN p.name, COLLECT (r.roles) AS characters
ORDER BY p.name


//#		Display all the movies in which an actor played multiple roles (characters)
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE SIZE(r.roles) > 1
RETURN m.title, p.name, r.roles
ORDER BY m.title, p.name


//#		Extract all the movies in which an actor played multiple roles (characters);
//   but display only the first and the last of these roles/characters
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE SIZE(r.roles) > 1
RETURN m.title, p.name, r.roles AS all_roles,
HEAD(r.roles) AS first_role,  LAST(r.roles) AS last_role
ORDER BY m.title, p.name


//# 	Display all the roles (characters) for all of the movies
MATCH () -[r:ACTED_IN]-> (m:Movie)
WITH r UNWIND r.roles AS movie_role
RETURN DISTINCT movie_role
ORDER BY movie_role


//# 	Display all the roles (characters) of all of the movies;
// 					for each role, display all of the movies it is played in
MATCH () -[r:ACTED_IN]-> (m:Movie)
WITH m, r UNWIND r.roles AS movie_role
RETURN movie_role, COLLECT (m.title) AS movies
ORDER BY movie_role


//# 	Display only the roles (characters) that appears in two of more movies;
MATCH () -[r:ACTED_IN]-> (m:Movie)
WITH m, r UNWIND r.roles AS movie_role
WITH movie_role, COLLECT (m.title) AS movies
WHERE SIZE(movies) >= 2
RETURN movie_role, movies, SIZE(movies)
ORDER BY movie_role


//# 	Display the oldest and the youngest people in the database?

// ... another solution based on collections
MATCH (p1:Person)
WITH p1.born AS years_init
ORDER BY p1.born
WITH COLLECT (years_init) AS years_coll
WITH HEAD(years_coll) AS first_year, LAST(years_coll) AS last_year
MATCH(p2:Person)
WHERE p2.born IN [first_year, last_year]
RETURN p2.name, p2.born


//# 	Display the people born in the first two years (of the years of
//   birth for the people in the database)?

// another solution based on collections
MATCH (p1:Person)
WITH p1.born AS years_init
ORDER BY p1.born
WITH COLLECT (years_init) AS years_coll
WITH years_coll[0] AS first_year, years_coll[1] AS second_year
MATCH(p2:Person)
WHERE p2.born IN [first_year, second_year]
RETURN p2.name, p2.born


//# 	Retrieve movies with role/character "Zachry" or "Dr. Henry Goose"
MATCH () -[r:ACTED_IN]-> (m:Movie)
WHERE ANY (role IN r.roles WHERE role IN ['Zachry', 'Dr. Henry Goose'])
RETURN m.title, r.roles


//# 	Retrieve movies in which only one from  "Zachry" or "Dr. Henry Goose"
//  role/character appears
MATCH () -[r:ACTED_IN]-> (m:Movie)
WHERE SINGLE (role IN r.roles WHERE role IN ['Zachry', 'Dr. Henry Goose'])
RETURN m.title, r.roles


//# 	Retrieve movies in which neither "Zachry" nor "Dr. Henry Goose"
//  role/character appears
MATCH () -[r:ACTED_IN]-> (m:Movie)
WHERE NONE (role IN r.roles WHERE role IN ['Zachry', 'Dr. Henry Goose'])
RETURN m.title, r.roles


//# 	Retrieve movies in which all "DeDe", "Angelica Graynamore",
//  and "Patricia Graynamore" roles/characters appear
MATCH () -[r:ACTED_IN]-> (m:Movie)
WHERE ALL (role IN r.roles WHERE role IN ["DeDe", "Angelica Graynamore", "Patricia Graynamore"])
RETURN m.title, r.roles
