//###################################################################################
//### 													     Movies
//###                This is a Neo4j Desktop built-in dataset
//###################################################################################
//### last update: 2021-03-18



//###################################################################################
// 	Notice: All of the next queries must be launched one by one in the Neo4j browser
//###################################################################################



//###################################################################################
//###								          Aggregation with groups


>>>>>>>>>>>>>>>>>???

//# 	Display all people and the the number of movies she/he directed
MATCH (p:Person) -[r]-> (m:Movie)
RETURN p.name, type(r), COUNT(m) AS n_of_movies
ORDER BY p.name, type(r)



//# 	Display each publisher's number of books, including category of books without publisher
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
RETURN COALESCE(p.name, 'Books without publisher') AS publisher_name, COUNT(b) AS n_of_books
ORDER BY publisher_name


//# 	Display each publisher's number of books, including category of books without publisher
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
RETURN COALESCE(p.name, 'Books without publisher') AS publisher_name, COUNT(b) AS n_of_books
ORDER BY publisher_name


//# 	Display the number of books published by Packt and the rest of the publishers (all but Packt)
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
RETURN
	CASE
	WHEN COALESCE(p.name, 'Books without publisher') = 'Packt Publishing' THEN 'Packt'
		ELSE 'other' END AS publisher_name, COUNT(b) AS n_of_books

//...and a solution based on UNION
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WHERE p.name = 'Packt Publishing'
RETURN p.name AS publisher_name, COUNT(b) AS n_of_books
UNION
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
WHERE COALESCE(p.name, ' ') <> 'Packt Publishing'
RETURN 'other' AS publisher_name, COUNT(b) AS n_of_books


//# 	Display publishers with at least two books  (GROUP BY... HAVING)
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
WHERE n_of_books >= 2
RETURN p.name  AS publisher_name, n_of_books
ORDER BY publisher_name


//# 	Display publishers with at least two books  (GROUP BY... HAVING)
//... another version that uses two "WITH"s
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
WHERE n_of_books >= 2
WITH p, n_of_books
RETURN *


//# 	Display the years of the books for the publisher with the greatest number
//   		of published books
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
ORDER BY n_of_books DESC
LIMIT 1
WITH p
MATCH (p) <-[r2:PublishedBy]-> ()
RETURN DISTINCT r2.year
ORDER BY r2.year






>>>>>
// Max de Marzi
MATCH (actor:Actor {name:"Tom Hanks"})-[:ACTED_IN]->(movie), (other:Actor)
WHERE NOT (movie)<-[:ACTED_IN]-(other)
RETURN other

// Wes Freeman
MATCH (actor:Actor {name:"Tom Hanks"}), (other:Actor)
WHERE NOT (actor)-[:ACTED_IN]->()<-[:ACTED_IN]-(other)
RETURN other


// Michael Hunger
MATCH (actor:Actor {name:"Tom Hanks"} )-[:ACTED_IN]->(movies)<-[:ACTED_IN]-(coactor)
WITH collect(distinct coactor) as coactors
MATCH (actor:Actor)
WHERE NOT actor IN coactors
RETURN actor



//# 	Display publishers with at least two books  (GROUP BY... HAVING)
//... another version that uses two "WITH"s
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
WHERE n_of_books >= 2
WITH p, n_of_books
RETURN *


>>>>>



CREATE (ThePolarExpress:Movie {title:'The Polar Express', released:2004, tagline:'This Holiday Season… Believe'})
CREATE
(TomH)-[:ACTED_IN {roles:['Hero Boy', 'Father', 'Conductor', 'Hobo', 'Scrooge', 'Santa Claus']}]->(ThePolarExpress),
(RobertZ)-[:DIRECTED]->(ThePolarExpress)




//###################################################################################
//###								Aggregate queries


//# 	How many books are in the database ?
MATCH (b:Book)
RETURN count(*) AS n_of_books


//#		How many books have specified the publisher?
MATCH (b:Book)-[r:PublishedBy]-> (p:Publisher)
RETURN COUNT(b)


//# 	Display each book's number of authors
MATCH (b:Book)  <-[a:AuthorOf]- ()
RETURN b.title, COUNT(a) AS n_of_authors
ORDER BY b.title


//# 	Display each publisher's number of books
MATCH (b:Book)-[r:PublishedBy]-> (p:Publisher)
RETURN p, COUNT(b) AS n_of_books


//# 	Display each author's number of books
MATCH (cp:Book)  <-[a:AuthorOf]- (auth:Person)
RETURN auth.surname, auth.name, COUNT(*) AS n_of_books
ORDER BY auth.surname, auth.name


//# 	Display each publisher's number of books, including category of books without publisher
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
RETURN COALESCE(p.name, 'Books without publisher') AS publisher_name, COUNT(b) AS n_of_books
ORDER BY publisher_name


//# 	Display each publisher's number of books, including category of books without publisher
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
RETURN COALESCE(p.name, 'Books without publisher') AS publisher_name, COUNT(b) AS n_of_books
ORDER BY publisher_name


//# 	Display the number of books published by Packt and the rest of the publishers (all but Packt)
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
RETURN
	CASE
	WHEN COALESCE(p.name, 'Books without publisher') = 'Packt Publishing' THEN 'Packt'
		ELSE 'other' END AS publisher_name, COUNT(b) AS n_of_books

//...and a solution based on UNION
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WHERE p.name = 'Packt Publishing'
RETURN p.name AS publisher_name, COUNT(b) AS n_of_books
UNION
MATCH (b:Book)
OPTIONAL MATCH (b) -[r:PublishedBy]-> (p:Publisher)
WHERE COALESCE(p.name, ' ') <> 'Packt Publishing'
RETURN 'other' AS publisher_name, COUNT(b) AS n_of_books


//# 	Display publishers with at least two books  (GROUP BY... HAVING)
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
WHERE n_of_books >= 2
RETURN p.name  AS publisher_name, n_of_books
ORDER BY publisher_name


//# 	Display publishers with at least two books  (GROUP BY... HAVING)
//... another version that uses two "WITH"s
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
WHERE n_of_books >= 2
WITH p, n_of_books
RETURN *


//# 	Display the years of the books for the publisher with the greatest number
//   		of published books
MATCH (b:Book) -[r:PublishedBy]-> (p:Publisher)
WITH p, COUNT(b) AS n_of_books
ORDER BY n_of_books DESC
LIMIT 1
WITH p
MATCH (p) <-[r2:PublishedBy]-> ()
RETURN DISTINCT r2.year
ORDER BY r2.year



//###################################################################################
//###							Queries with collections


//# 	Display the number of tags of each book
MATCH (book:Book)
RETURN book, LENGTH(book.tags) AS n_of_tags


//# 	display 10 most tagged books (books with highest number of tags)
MATCH (book:Book)
WHERE LENGTH(book.tags) > 0
RETURN book.title, book.tags
ORDER BY LENGTH(book.tags) DESC
LIMIT 10


//# 	Display the first and the last tag for the
//			10 most tagged books (books with highest number of tags)
MATCH (book:Book) WHERE LENGTH(book.tags) > 0
RETURN book.title, book.tags, HEAD(book.tags), LAST(book.tags)
ORDER BY LENGTH(book.tags) DESC
LIMIT 10


//# 	Display the first two tags for the
//			10 most tagged books (books with highest number of tags)
MATCH (book:Book) WHERE LENGTH(book.tags) > 0
RETURN book.title, book.tags, HEAD(book.tags), HEAD(TAIL(book.tags))
ORDER BY LENGTH(book.tags) DESC
LIMIT 10


//# 	Retrieve books covering "neo4j" or "nosql"
MATCH (book:Book)
WHERE ANY (tag IN book.tags WHERE tag IN ['neo4j', 'nosql'])
RETURN *


//# 	Retrieve books covering only one from "neo4j" and "nosql" (along with other subjects/tags)
MATCH (book:Book)
WHERE SINGLE  (tag IN book.tags WHERE tag IN ['neo4j', 'nosql'])
RETURN *


//# 	Retrieve books covering neither "neo4j" nor "nosql"
MATCH (book:Book)
WHERE NONE  (tag IN book.tags WHERE tag IN ['neo4j', 'nosql'])
RETURN *


//# 	Retrieve books covering "neo4j" or "nosql" and nothing else
MATCH (book:Book)
WHERE ALL (tag IN book.tags WHERE tag IN ['neo4j', 'nosql'])
	AND book.tags IS NOT NULL
RETURN *


//# 	Display each author's book tags
MATCH (cp:Book)  <-[a:AuthorOf]- (auth:Person)
RETURN auth.surname, auth.name, COLLECT(cp.tags) AS Tags
ORDER BY auth.surname, auth.name


//#		Retrieve all tags in the database
//...
MATCH (book:Book)
WHERE book.tags IS NOT NULL
WITH  book UNWIND book.tags AS tag
RETURN DISTINCT tag
ORDER BY tag


//...or:
MATCH (book:Book)
WHERE book.tags IS NOT NULL
WITH  book UNWIND book.tags AS tag
WITH DISTINCT tag
RETURN tag
ORDER BY tag


//# 	Retrieve books covering "mongodb"

//... (above) solution with ANY
MATCH (book:Book)
WHERE ANY (tag IN book.tags WHERE tag IN ['mongodb'])
RETURN *

//... and another based on UNWIND (similar to MongoDB)
MATCH (book:Book)
WHERE book.tags IS NOT NULL
WITH  book.title as title, book UNWIND book.tags AS tag
WITH title, tag
WHERE tag = 'mongodb'
RETURN *


//#		Retrieve the most frequent tag
MATCH (book:Book)
WHERE book.tags IS NOT NULL
WITH  book.title as title, book UNWIND book.tags AS tag
WITH tag, COUNT(title) AS n_of_books
RETURN tag, n_of_books
ORDER BY n_of_books DESC
LIMIT 1


//#		Display the tags for each author, but without tag duplication
//  (as was the case when we displayed each author's book tags)
MATCH (book:Book)  <- [a:AuthorOf]- (auth:Person)
WHERE book.tags IS NOT NULL
WITH  auth, book UNWIND book.tags AS tag
WITH auth, tag
RETURN auth.name, auth.surname, COLLECT (DISTINCT tag) AS tags
ORDER BY auth.surname, auth.name


//#		Retrieved the most tagged author
MATCH (book:Book)  <- [a:AuthorOf]- (auth:Person)
WHERE book.tags IS NOT NULL
WITH  auth, book UNWIND book.tags AS tag
WITH auth, tag
RETURN auth.name, auth.surname, COUNT (DISTINCT tag) AS n_of_tags
ORDER BY n_of_tags DESC LIMIT 1




//###################################################################################
//###							Edit nodes and relations


//# 	Declare a new property (edition_no) for book "MongoDB in action" (add a property to a node)
MATCH (b:Book)
WHERE b.title = "MongoDB in action"
SET b.edition_no = 2
RETURN b


//# 	Declare URL for book "MongoDB in action" (add a property to a node)
MATCH (b:Book)
WHERE b.title = "MongoDB in action"
SET b.URL = "https://www.manning.com/books/mongodb-in-action-second-edition"
RETURN b


//#		Remove a property of a node
MATCH (b:Book)
WHERE b.title = "MongoDB in action"
SET b.URL = NULL
RETURN b


//# 	Delete all existing nodes and relationships
MATCH (n)
OPTIONAL MATCH (n)-[r]-()
DELETE n,r
