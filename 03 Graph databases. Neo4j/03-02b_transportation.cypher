//############################################################################
//### 			    Mini Case Study Cypher Neo4j: Transportation
//###				   B. Querying the database
//#############################################################################
//### last update: 2021-03-18


//#######################################################################################
//###			Simple queries retrieving and displaying nodes and relations
//###
//### Queries must be launched one by one in browser

//# Get some data
MATCH (n) RETURN n LIMIT 20


//# 	Display only the labels (types) of nodes in the database
MATCH (n)
RETURN DISTINCT labels(n)


//#	 	Display all the nodes in the database
MATCH (n)
RETURN n, labels(n)


//# 	Display all the City nodes (with their properties)
MATCH (n:City)
RETURN n


//# 	Display all the city names and their population
MATCH (n:City)
RETURN n.cityName, n.population
ORDER BY n.cityName

//#	Display just two nodes referring to cities of Iasi and Bacau
MATCH (n1:City {cityName: 'Iasi'}), (n2:City {cityName: 'Bacau'})
RETURN n1, n2



//# 	Display all the counties in region of "Moldova"

// v1
MATCH (county:County) -[rel:IS_IN_REGION]-> (region:Region)
WHERE region.regionName = 'Moldova'
RETURN county, rel, region

// v2
MATCH (county:County) -[rel:IS_IN_REGION]-> (region:Region {regionName : 'Moldova' } )
RETURN county, rel, region

// v3
MATCH (county:County) --> (region:Region {regionName : 'Moldova' } )
RETURN county, region


//# 	Display city names and counties;
//     results will be ordered by countries and,
//       within counties, by city names
MATCH (city:City) -[rel:IS_IN_COUNTY]->(county:County)
RETURN city.cityName, county.countyName
ORDER BY county.countyName, city.cityName

// the same result, but this time relationships are included
MATCH (city:City) -[rel:IS_IN_COUNTY]->(county:County)
RETURN city.cityName, type(rel), county.countyName
ORDER BY county.countyName, city.cityName


//# 	Display the cities in "Vrancea" county

// first solution (only nodes are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County {countyName : 'Vrancea'} )
RETURN city.cityName, county.countyName
ORDER BY city.cityName

// second solution (only nodes are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
WHERE county.countyName = 'Vrancea'
RETURN city.cityName, county.countyName
ORDER BY city.cityName

// third solution (all nodes are relationship are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
WHERE county.countyName = 'Vrancea'
RETURN *
ORDER BY city.cityName

// fourth solution (all nodes only one relationship are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
WHERE county.countyName = 'Vrancea'
RETURN city, relCounty, county
ORDER BY city.cityName


//# In which county city of Adjud is placed on ?
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
WHERE city.cityName = 'Adjud'
RETURN county.countyName


//# Which are the cities in the same county as the city of Adjud ?
MATCH
	(city1:City)  -[relCounty:IS_IN_COUNTY]-> (county:County)
		<- [relCounty2:IS_IN_COUNTY]-  (city2:City)
WHERE city1.cityName = 'Adjud'
RETURN city2.cityName

// the same result, but display all the nodes and relationships
MATCH
	(city1:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		<- [relCounty2:IS_IN_COUNTY]-
	(city2:City)
WHERE city1.cityName = 'Adjud'
RETURN *


//# 	Display the cities from region of "Moldova"

// 	first solution (only nodes are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region{ regionName: 'Moldova' })
RETURN city.cityName, county.countyName, region.regionName
ORDER BY city.cityName

//	second solution (only nodes are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
WHERE region.regionName = 'Moldova'
RETURN city.cityName, county.countyName, region.regionName
ORDER BY city.cityName

//	third solution (all nodes are relationship are displayed):
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
WHERE region.regionName = 'Moldova'
RETURN *
ORDER BY city.cityName


//# In which region the city of Adjud is placed on ?
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
WHERE city.cityName = 'Adjud'
RETURN region.regionName


//# Which are the cities in the same region as the city of Adjud ?
MATCH
	(city1:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
		<- [relRegion2:IS_IN_REGION]-
	(county2:County)
		<- [relCounty2:IS_IN_COUNTY]-
	(city2:City)
WHERE city1.cityName = 'Adjud'
RETURN city2.cityName

// the same problem, but display all the nodes and relationships
MATCH
	(city1:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
		<- [relRegion2:IS_IN_REGION]-
	(county2:County)
		<- [relCounty2:IS_IN_COUNTY]-
	(city2:City)
WHERE city1.cityName = 'Adjud'
RETURN *



//#####################################################################################
//###						Basic aggregate queries
//###

//# 	How many cities are in the "Moldova" region?
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region{ regionName: 'Moldova' })
RETURN count(*)


//# 	Display the number of cities in every county of "Moldova" region
//     (GROUP BY)
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region{ regionName: 'Moldova' })
RETURN county.countyName, count(*)


//# 	Display the number of cities in every county of every region
//     (GROUP BY two attributes)

MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
RETURN region.regionName, county.countyName, count(*)
ORDER BY region.regionName, county.countyName



//##################################################################################
//###							Graph queries (1): Basics
//###

//#		Display the neighbour cities of Iasi, excluding the anchor
//      (path departure node - Iasi)
//	 (first order neighbours)
MATCH
	(n:City { cityName: 'Iasi' })
		-[:CONNECTED_TO*1]-
	(neighborhood)
RETURN neighborhood


//#		Display the neighbour cities of Iasi, including the anchor
//      (path departure node - Iasi)
//	 (first order neighbours)
MATCH
	(n:City { cityName: 'Iasi' })
		-[:CONNECTED_TO*0..1]->
	(neighborhood)
RETURN neighborhood


//#		Display the neighbor cities of Iasi (first order neighbours)
// 	with more than 30000 inhabitants (filter nodes)
//  	(filter the path nodes)
MATCH
	(n:City { cityName: 'Iasi' })
		-[:CONNECTED_TO*1]-
	(neighborhood)
WHERE neighborhood.population >= 30000
RETURN *


//#		Display the neighbor cities of Iasi (first order neighbours)
//   found at exactly 53 km from Iasi (filter relationships - equality operator)
//	  (filter the path relationship)
MATCH
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*1{distance : 53}]-
	(neighborhood)
RETURN *


//#		Display the neighbor cities of Iasi (first order neighbours)
//   within less than 60 kilometers
//  (filter the path relationship - "greater-than" operator)
MATCH
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*1]->
	(neighborhood)
WHERE rel.distance <= 60
RETURN *

// it does not work; see below: collections


//#		Display only the neighbors of the neighbours of Iasi
//	 (second order neighbours)
MATCH
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*2]->
	(neighborhood)
RETURN neighborhood


//#		Display only the third and fourth order neighbours of Iasi
MATCH
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*3..4]-
	(neighborhood)
RETURN neighborhood

//#		Display only the third and fourth order neighbours of Iasi,
//      this time excluding Iasi (as intermediary or final path node)
MATCH
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*3..4]->
	(neighborhood)
WHERE neighborhood.cityName <> 'Iasi'
RETURN neighborhood



//##############################################################################
//###						Graph queries (2): Paths
//###


//#		Display all the paths from Iasi to Bacau but
//			with no more than ONE intermediary node
//     see cardinality [rel:CONNECTED_TO*0..2]
MATCH p =
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*0..2]-
	(neighborhood)
WHERE neighborhood.cityName = 'Bacau'
RETURN p


//#		Display all the paths from Iasi to Bacau
//			with no more than TWO intermediary nodes
//      see cardinality [rel:CONNECTED_TO*0..3]
MATCH p =
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*0..3]-
	(neighborhood)
WHERE neighborhood.cityName = 'Bacau'
RETURN p


//#		Display all the paths from Iasi to Bacau with
//			exactly TWO intermediary nodes
//     see cardinality [rel:CONNECTED_TO*3..3]
MATCH p =
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*3..3]-
	(neighborhood)
WHERE neighborhood.cityName = 'Bacau'
RETURN p


//#		Display all the paths from Iasi to Bacau
//			with no more than FIVE intermediary nodes
//     see cardinality [rel:CONNECTED_TO*0..6]
MATCH p =
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*0..6]-
	(neighborhood)
WHERE neighborhood.cityName = 'Bacau'
RETURN p


//#		Display all the paths from Iasi to Bacau with
//			exactly FIVE intermediary nodes
//     see cardinality [rel:CONNECTED_TO*6..6]
MATCH p =
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*6..6]-
	(neighborhood)
WHERE neighborhood.cityName = 'Bacau'
RETURN p



//#		Display the shortest path (as number of nodes) from Iasi to Bacau

//		syntax 1
MATCH p = SHORTESTPATH(
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*]-
	(neighborhood{ cityName: 'Bacau' }) )
RETURN p

//		syntax 2
MATCH
	(n1 {cityName: 'Iasi'}), (n2 {cityName: 'Bacau'})
RETURN allShortestPaths( (n1) -[:CONNECTED_TO*]- (n2) ) as path


//# there is only one shortest path (as number of nodes), so next query
//#		produces the same result
MATCH p = ALLSHORTESTPATHS(
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*]-
	(neighborhood{ cityName: 'Bacau' }) )
RETURN p


//# 	compare with 'Iasi' - 'Tisita' paths
MATCH p = SHORTESTPATH(
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*]-
	(neighborhood{ cityName: 'Tisita' }) )
RETURN p

//	 vs.
MATCH p = ALLSHORTESTPATHS(
	(n:City { cityName: 'Iasi' })
		-[rel:CONNECTED_TO*]-
	(neighborhood{ cityName: 'Tisita' }) )
RETURN p




// #######################################################################
// ####   					Collections


//# 	Displays as collection all the cities in every county
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
RETURN county.countyName, COLLECT (city.cityName) AS cities


//# 	Display, for every region, all the counties and all the cities
MATCH
	(city:City)
		-[relCounty:IS_IN_COUNTY]->
	(county:County)
		-[relRegion:IS_IN_REGION]->
	(region:Region)
RETURN region.regionName, COLLECT (county.countyName) AS counties,
	COLLECT (city.cityName) AS cities


//#		Display the neighbor cities of Iasi (first order neighbors)
//#   within less than 60 kilometers (filter relationships)
//   UNWIND
MATCH (n:City { cityName: 'Iasi' }) -[r:CONNECTED_TO*1]- (neighborhood)
UNWIND (r) AS rs
WITH rs
WHERE rs.distance <= 60
RETURN *


//#		Display all paths from Iasi to Bacau shorter than 175 km
//  this time using collections and UNWIND
MATCH p = (n1 {cityName: 'Iasi'}) -[r:CONNECTED_TO*]- (n2 {cityName: 'Bacau'})
WITH  n1, r, n2,
	EXTRACT (node IN NODES(p) | node.cityName ) AS path,
	EXTRACT (rel IN RELATIONSHIPS(p) | rel.distance ) AS distances
WITH path, distances UNWIND distances AS length
WITH path, distances, SUM(length) AS total_distance
WHERE total_distance <= 175
RETURN *


//#			Display as text each path from Iasi to Bacau
//	  with no more than FIVE intermediary cities
MATCH p = (n1 {cityName: 'Iasi'}) -[:CONNECTED_TO*..6]- (n2 {cityName: 'Bacau'})
WITH REDUCE (text = ' ', nod IN NODES(p) |
	text + ' - ' + nod.cityName) as text
RETURN text


//#		Display routes from Iasi to Bacau with no more than FIVE intermediary nodes,
//   but, this time, displaying the length in kilometers of each route
MATCH p = (n1 {cityName: 'Iasi'}) -[:CONNECTED_TO*..6]- (n2 {cityName: 'Bacau'})
WITH REDUCE (text = ' ', nod IN NODES(p) | text + ' - ' + nod.cityName) as text,
 REDUCE (dist = 0, rel IN RELATIONSHIPS(p) | dist + rel.distance) as total_distance
RETURN text, total_distance


//#		Display all the routes from Iasi to Bacau in ascending order of total distance
MATCH p = (n1 {cityName: 'Iasi'}) -[:CONNECTED_TO*]- (n2 {cityName: 'Bacau'})
WITH REDUCE (text = ' ', nod IN NODES(p) | text + ' - ' + nod.cityName) as text,
 REDUCE (dist = 0, rel IN RELATIONSHIPS(p) | dist + rel.distance) as total_distance
RETURN text, total_distance
ORDER BY total_distance


//#		Display all the routes from Iasi to Bacau in ascending order of total distance
//   but without passing through node more than once
// solution was suggested by Michael Hunger at:
// http://stackoverflow.com/questions/28261198/finding-cypher-paths-that-dont-visit-the-same-node-twice
//
MATCH p = (n1 {cityName: 'Iasi'}) -[:CONNECTED_TO*]- (n2 {cityName: 'Bacau'})
WHERE NONE (n IN nodes(p)
	WHERE size(filter(x IN nodes(p)
		WHERE n = x))> 1)
WITH REDUCE (text = ' ', nod IN NODES(p) | text + ' - ' + nod.cityName) as text,
 REDUCE (dist = 0, rel IN RELATIONSHIPS(p) | dist + rel.distance) as total_distance
RETURN text, total_distance
ORDER BY total_distance


//#		Display Display the shortest route (in terms of distance) from Iasi to Bacau
MATCH p = (n1 {cityName: 'Iasi'}) -[:CONNECTED_TO*]- (n2 {cityName: 'Bacau'})
WITH REDUCE (text = ' ', nod IN NODES(p) | text + ' - ' + nod.cityName) as text,
 REDUCE (dist = 0, rel IN RELATIONSHIPS(p) | dist + rel.distance) as total_distance
RETURN text, total_distance
ORDER BY total_distance
LIMIT 1


//#		Display all paths from Iasi to Bacau shorter than 175 km
MATCH p = (n1 {cityName: 'Iasi'}) -[:CONNECTED_TO*]- (n2 {cityName: 'Bacau'})
WITH REDUCE (text = ' ', nod IN NODES(p) | text + ' - ' + nod.cityName) as text,
 REDUCE (dist = 0, rel IN RELATIONSHIPS(p) | dist + rel.distance) as total_distance
WITH text, total_distance
WHERE total_distance <= 175
RETURN text, total_distance
ORDER BY total_distance



//#####################################################################################
//###						Advanced aggregation queries
//###

//# 	Display the counties with more than one city in the database
//   (GROUP BY and HAVING)
MATCH
	(city:City) -[relCounty:IS_IN_COUNTY]-> (county:County)
WITH county.countyName AS countyName, count(*) AS n_of_cities
WHERE n_of_cities > 1
RETURN *


//# 	Display the county with the greatest number of cities

// this works fine where there is only one county with the greatest number of cities
MATCH  (city:City) -[relCounty:IS_IN_COUNTY]-> (county:County)
RETURN county.countyName, count(*)
ORDER BY count(*) DESC LIMIT 1


// next solution works fine even if there are two or more counties with the same max number of cities,
//    we need another solution - see below

MATCH (city:City) -[relCounty:IS_IN_COUNTY]->(county:County)
WITH county.countyName AS countyName, count(*) AS n_of_cities
WITH MAX(n_of_cities) AS max_n_of_cities
WITH max_n_of_cities
MATCH  (city:City) -[relCounty:IS_IN_COUNTY]->(county:County)
WITH max_n_of_cities, county.countyName AS countyName, count(*) AS n_of_cities
WHERE n_of_cities = max_n_of_cities
RETURN *
