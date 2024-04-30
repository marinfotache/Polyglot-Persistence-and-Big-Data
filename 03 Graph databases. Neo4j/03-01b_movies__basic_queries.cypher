//###################################################################################
//### 													     Movies (part 2)
//###                This is a Neo4j Desktop built-in dataset
//###################################################################################
//### last update: 2024-04-16


//#######################  		DB administration stuff		##################
// launch the previous script (`03-01a...`) in order to setup `movies` graph


//###################################################################################
// 	      Notice: All of the queries (in this script and subsequent ones)
//                  must be launched one by one in the Neo4j browser
//###################################################################################

call db.schema.visualization ;


//###################################################################################
//###			             Simple queries retrieving and displaying nodes
//###################################################################################


//# 	Display all types (labels) of nodes in the database
MATCH (x)
RETURN DISTINCT labels(x)


//# 	Display all the movies (all the nodes tagged "Movie")
MATCH (x:Movie)
RETURN x


//# 	Display only the title of the movies (property "title")
MATCH (x:Movie)
RETURN x.title


//# 	Display titles of the movies in alphabetic order
MATCH (x:Movie)
RETURN x.title
ORDER BY x.title


//# 	Display first 5 movie titles in alphabetic order
MATCH (x:Movie)
RETURN x.title
ORDER BY x.title
LIMIT 5


//# 	Display 7 movie titles in alphabetic order, from the 5th to the 12th
MATCH (x:Movie)
RETURN x.title
ORDER BY x.title
SKIP 4
LIMIT 7


//# 	Display the movies released in 1986
MATCH (x:Movie)
WHERE x.released = 1986
RETURN *

//... another solution:
MATCH (x:Movie {released : 1986})
RETURN *


//# 	Display the movies released after 2000
MATCH (x:Movie)
WHERE x.released > 2000
RETURN *


//# 	Display the movies released between 2002 and 2004
MATCH (x:Movie)
WHERE x.released IN [2002, 2003, 2004]
RETURN *


//# 	Display the movies of `The Matrix` series
MATCH (x:Movie)
WHERE x.title =~ 'The Matrix.*'
RETURN x.title
ORDER BY x.title


//# 	Display the movies whose title contains `Matrix`
MATCH (x:Movie)
WHERE x.title =~ '.*[Mm]atrix.*'
RETURN x.title
ORDER BY x.title


//##	 Display all the nodes in the database with their id and labels
//    	Result should be order by the first label of the node  and then by
//       name (if the node represents a person) or by title
//       (if the node represents a movie)
MATCH (x)
RETURN ID(x), labels(x), x.title, x.name
ORDER BY labels(x)[0], x.title, x.name


//#		Retrieve only movies without a value for attribute `tagline`
MATCH (x:Movies)
WHERE x.tagline IS NULL
RETURN *




//###################################################################################
//###	             Simple queries involving relationships
//###################################################################################

//##	 Display all the relations in the database
MATCH (n1)-[r]-(n2)
RETURN r;
//# Result is not so useful; see next query


//##	 Display all the relation types in the database
MATCH (n1)-[r]-(n2)
RETURN DISTINCT type(r)
ORDER BY type(r);


//##	 Display all the nodes and all the relationships in the database
MATCH (n)-[r]->(m)
RETURN n,r,m;


//## 	Display the movie director for the movie `As Good as It Gets`
MATCH (p:Person) -[r:DIRECTED]-> (m:Movie)
WHERE m.title = 'As Good as It Gets'
RETURN p.name, type(r), m.title


//## Find the years when the movies casting Jack Nicholson were released
MATCH (j:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE j.name = 'Jack Nicholson' 
RETURN DISTINCT m.released
ORDER BY m.released


//## 	Display the movie director(s) and producer(s) for the movie `The Matrix`
MATCH (p:Person) -[r:DIRECTED|PRODUCED]-> (m:Movie)
WHERE m.title = 'The Matrix'
RETURN p.name, type(r), m.title


//## 	Display the movie directors and producers for the `Matrix` series
MATCH (p:Person) -[r:DIRECTED|PRODUCED]-> (m:Movie)
WHERE m.title =~ '.*[Mm]atrix.*'
RETURN p.name, type(r), m.title


//## 	Display the movies directed or produced by 'Lilly Wachowski'
MATCH (p:Person) -[r:DIRECTED|PRODUCED]-> (m:Movie)
WHERE p.name =~ 'Lilly Wachowski'
RETURN p.name, type(r), m.title


//## Display the movie titles on which Tom Hanks was casted
MATCH (p:Person {name : 'Tom Hanks'}) -[r:ACTED_IN]-> (m:Movie)
RETURN m.title
ORDER BY m.title


//## 	Display all the movies in which 'Lilly Wachowski' contributed
//  (no matter how - director, producer...)
MATCH (p:Person) -[r]-> (m:Movie)
WHERE p.name = 'Lilly Wachowski'
RETURN p.name, type(r), m.title


//## 	Display all types of contribution to the movies for 'Lilly Wachowski'
MATCH (p:Person) -[r]-> (m:Movie)
WHERE p.name = 'Lilly Wachowski'
RETURN DISTINCT type(r)
ORDER BY type(r)


//## 	Display as a graph the cast of movies `As Good as It Gets`
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE m.title = 'As Good as It Gets'
RETURN *


//## 	Which role (character) Helen Hunt played in `As Good as It Gets`?
MATCH (p:Person) -[r:ACTED_IN]-> (m:Movie)
WHERE p.name = 'Helen Hunt' AND m.title = 'As Good as It Gets'
RETURN r.roles


//## (sort of) LEFT JOIN in Cypher !
MATCH (p:Person)
OPTIONAL MATCH (p) -[r:ACTED_IN]-> (m:Movie)
RETURN p.name, type(r), m.title
ORDER BY p.name




//###################################################################################
//###	             Queries involving a chain of nodes and relationships
//###################################################################################


//## 	Display the actors who played in movies along with Keanu Reeves

// sol.1
MATCH (p:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
WHERE keanu.name = 'Keanu Reeves'
RETURN p.name, m.title, keanu.name

// sol.2
MATCH (p:Person) -[r1:ACTED_IN]-> (m:Movie)
MATCH (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
WHERE keanu.name = 'Keanu Reeves' AND p.name <> 'Keanu Reeves'
RETURN DISTINCT p.name, m.title, keanu.name

// sol.3
MATCH (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
MATCH (p:Person) -[r1:ACTED_IN]-> (m:Movie)
WHERE keanu.name = 'Keanu Reeves' AND p.name <> 'Keanu Reeves'
RETURN DISTINCT p.name, m.title, keanu.name

// sol.4
MATCH (keanu:Person)
WHERE keanu.name = 'Keanu Reeves'
MATCH (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
MATCH (p:Person) -[r1:ACTED_IN]-> (m:Movie)
WHERE p.name <> 'Keanu Reeves'
RETURN DISTINCT p.name, m.title, keanu.name



//## 	Display the directors of the movies featuring Keanu Reeves

// sol.1
MATCH (p:Person) -[r1:DIRECTED]-> (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
WHERE keanu.name = 'Keanu Reeves'
RETURN p.name, type(r1), m.title, keanu.name

// sol.2
MATCH (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
MATCH (p:Person) -[r1:DIRECTED]-> (m:Movie)
WHERE keanu.name = 'Keanu Reeves'
RETURN p.name, type(r1), m.title, keanu.name

// sol.3
MATCH (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
WHERE keanu.name = 'Keanu Reeves'
MATCH (p:Person) -[r1:DIRECTED]-> (m:Movie)
RETURN p.name, type(r1), m.title, keanu.name

// sol.4
MATCH (keanu:Person)
WHERE keanu.name = 'Keanu Reeves'
MATCH (m:Movie) <-[r2:ACTED_IN]- (keanu:Person)
MATCH (p:Person) -[r1:DIRECTED]-> (m:Movie)
RETURN DISTINCT p.name, m.title, keanu.name


//###################################################################################
//##                            Intersection in Cypher
//###################################################################################

//## 	Display the movies in which Keanu Reeves played with Carrie-Anne Moss

// sol.1 (result as text)
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (carrie:Person)
WHERE keanu.name = 'Keanu Reeves' AND carrie.name = 'Carrie-Anne Moss'
RETURN keanu.name, m.title, carrie.name

// sol.2 (result as graph)
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (carrie:Person)
WHERE keanu.name = 'Keanu Reeves' AND carrie.name = 'Carrie-Anne Moss'
RETURN *

// sol.3 
MATCH (keanu:Person) 
WHERE keanu.name = 'Keanu Reeves' 
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (carrie:Person)
WHERE carrie.name = 'Carrie-Anne Moss'
RETURN keanu.name, m.title, carrie.name

// sol.4 
MATCH (keanu:Person) 
WHERE keanu.name = 'Keanu Reeves' 
MATCH (carrie:Person) 
WHERE carrie.name = 'Carrie-Anne Moss'
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (carrie:Person)
RETURN keanu.name, m.title, carrie.name

// sol.5
MATCH (keanu:Person) 
WHERE keanu.name = 'Keanu Reeves' 
MATCH (carrie:Person) 
WHERE carrie.name = 'Carrie-Anne Moss'
MATCH (keanu) -[r1:ACTED_IN]-> (m:Movie) 
MATCH (m:Movie) <-[r2:ACTED_IN]- (carrie)
RETURN keanu.name, m.title, carrie.name

// sol.6
MATCH (keanu:Person {name:"Keanu Reeves"}) -[:ACTED_IN]-> (movie_keanu:Movie)
MATCH (carrie:Person {name:"Carrie-Anne Moss"})-[:ACTED_IN]-> (movie_carrie:Movie)
WHERE movie_keanu.title IN movie_carrie.title
RETURN DISTINCT movie_keanu.title



//## 	Display the actors who played in movies along with actors who, at their turn,
// played in movies with Keanu Reeves

// sol.1
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie) <-[r2:ACTED_IN]- (keanu_colleague:Person) -[r3:ACTED_IN]-> (m2:Movie) <- [r4:ACTED_IN]- (colleague_of_keanu_colleagues:Person)
WHERE keanu.name = 'Keanu Reeves' 
  AND keanu_colleague.name <> 'Keanu Reeves' 
  AND colleague_of_keanu_colleagues.name <> 'Keanu Reeves'
  AND keanu_colleague.name <> colleague_of_keanu_colleagues.name
  AND m.title <> m2.title
RETURN keanu.name, m.title, keanu_colleague.name, m2.title, colleague_of_keanu_colleagues.name
ORDER BY keanu.name, m.title, keanu_colleague.name, m2.title, colleague_of_keanu_colleagues.name
// 87 records

// sol.2
MATCH (k:Person) 
WHERE k.name = 'Keanu Reeves'
MATCH (k:Person) -[r1:ACTED_IN]-> (m1:Movie)
MATCH (k_colleague:Person) -[r2:ACTED_IN]-> (m1:Movie)
WHERE k_colleague.name <> 'Keanu Reeves'
MATCH (k_colleague:Person) -[r3:ACTED_IN]-> (m2:Movie)
MATCH (k_colleague2:Person) -[r4:ACTED_IN]-> (m2:Movie)
WHERE k_colleague2.name <> 'Keanu Reeves' AND k_colleague.name <> k_colleague2.name
  AND m1.title <> m2.title
RETURN k.name, m1.title, k_colleague.name, m2.title, k_colleague2.name
// 87 records




//###################################################################################
//###	    Chaining the results among query steps - sol. 1 - cartesian product
//###################################################################################

//##          another type of solutions to intersection in Cypher

// Task: find movies Keanu Reevers played with Carrie-Anne Moss:
// in the next solution, the comma between `(keanu:Person {name:"Keanu Reeves"})- [:ACTED_IN]-> (movie_keanu:Movie)`
//  and `(carrie:Person {name:"Carrie-Anne Moss"})` stands as a sort of cartesian product
MATCH (keanu:Person {name:"Keanu Reeves"})- [:ACTED_IN]-> (movie_keanu:Movie),
  (carrie:Person {name:"Carrie-Anne Moss"})
WHERE (movie_keanu)<-[:ACTED_IN]-(carrie)
RETURN keanu.name, movie_keanu.title, carrie.name


// ## difference in Cypher

//## 	Display the movies in which Keanu Reeves did NOT play along with Carrie-Anne Moss
// notice `NOT`

// This solution DOES NOT work properly !!!
MATCH (keanu:Person {name:"Keanu Reeves"}) -[:ACTED_IN]-> (movie_keanu:Movie)
MATCH (carrie:Person {name:"Carrie-Anne Moss"})-[:ACTED_IN]-> (movie_carrie:Movie)
WHERE NOT movie_keanu.title  IN movie_carrie.title
RETURN DISTINCT movie_keanu.title

// This solution DOES NOT work at all! 
MATCH (keanu:Person {name:"Keanu Reeves"}) -[:ACTED_IN]-> (movie_keanu:Movie)
MATCH (carrie:Person {name:"Carrie-Anne Moss"})-[:ACTED_IN]-> (movie_carrie:Movie)
WHERE  movie_keanu.title NOT IN movie_carrie.title
RETURN DISTINCT movie_keanu.title

// sol. 1
MATCH 
  (keanu:Person {name:"Keanu Reeves"})- [:ACTED_IN]-> (movie_keanu:Movie),
  (carrie:Person {name:"Carrie-Anne Moss"})
WHERE NOT (movie_keanu)<-[:ACTED_IN]-(carrie)
RETURN movie_keanu.title


// sol. 2
MATCH (carrie:Person {name:"Carrie-Anne Moss"})
MATCH (keanu:Person {name:"Keanu Reeves"}) -[:ACTED_IN]-> (movie_keanu:Movie)
WHERE NOT (movie_keanu)<-[:ACTED_IN]-(carrie)
RETURN movie_keanu.title




//###################################################################################
//###	         Chaining the results among query steps - sol. 2 - WITH clause
//###################################################################################

//##               other solutions to intersection in Cypher

//## Task: find movies Keanu Reeves played with Carrie-Anne Moss:
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie)
WHERE keanu.name = 'Keanu Reeves'
WITH m
MATCH (carrie:Person) -[r2:ACTED_IN]-> (m)
WHERE carrie.name = 'Carrie-Anne Moss'
RETURN m.title

//...or
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m:Movie)
WHERE keanu.name = 'Keanu Reeves'
MATCH (carrie:Person) -[r2:ACTED_IN]-> (m)
WHERE carrie.name = 'Carrie-Anne Moss'
RETURN m.title


// ... and another solution...
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m_keanu:Movie)
WHERE keanu.name = 'Keanu Reeves'
WITH m_keanu
MATCH (carrie:Person) -[r2:ACTED_IN]-> (m_carrie)
WHERE carrie.name = 'Carrie-Anne Moss' AND
	m_carrie.title = m_keanu.title
RETURN m_keanu.title

//...or
MATCH (keanu:Person) -[r1:ACTED_IN]-> (m_keanu:Movie)
WHERE keanu.name = 'Keanu Reeves'
MATCH (carrie:Person) -[r2:ACTED_IN]-> (m_carrie)
WHERE carrie.name = 'Carrie-Anne Moss' AND
	m_carrie.title = m_keanu.title
RETURN m_keanu.title


//## 	Task: Display the actors who were directed by the directors of the movies 
// featuring Keanu Reeves
MATCH (keanu:Person) -[r1:ACTED_IN]-> (mov_keanu:Movie)
WHERE keanu.name = 'Keanu Reeves'
WITH mov_keanu
MATCH (directors:Person) -[r2:DIRECTED]-> (mov_keanu)
WITH directors
MATCH (directors:Person) -[r3:DIRECTED]-> (mov_directors)
WITH mov_directors
MATCH (actors:Person) -[r4:ACTED_IN]-> (mov_directors)
RETURN *


MATCH (keanu:Person) -[r1:ACTED_IN]-> (mov_keanu:Movie)
WHERE keanu.name = 'Keanu Reeves'
WITH keanu, mov_keanu
MATCH (directors:Person) -[r2:DIRECTED]-> (mov_keanu)
WITH keanu, mov_keanu, directors
MATCH (directors:Person) -[r3:DIRECTED]-> (mov_directors)
WITH keanu, mov_keanu, directors, mov_directors
MATCH (actors:Person) -[r4:ACTED_IN]-> (mov_directors)
RETURN *



