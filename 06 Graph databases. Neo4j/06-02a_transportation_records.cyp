//############################################################################
//### 			    Mini Case Study Cypher Neo4j: Transportation
//###				   A. Creating the nodes and the relationships
//#############################################################################


//#######################  		DB administration stuff		##################

//
// Prerequisites (for running all of the Cypher scripts):
//
// 0.a. Install `Neo4j Desktop` from:
//  https://neo4j.com/download/
//
// 0.b. Create a project
//
// 
// Prerequisites (for running the statements in this script):
//
// 1.a. Run `Neo4j Desktop` on your computer
//
// 1.b. Open the project created at step `0.b`
//
// 1.c. After opening the project, click on `Add Graph` box, then choose `Create a Local Graph`;
//		set `Graph name` property on `transportation`, fill in the password, then click on button `Create`
// 
// 1.d. After the database creation completed, press `Start` button in `books` box. 
//
// 1.e. After the database is started, press `Manage` button, then `Open browser` button
//
//
// NOTICE:
// In subsequent sessions, as the database is created, you'll have to cover only steps
//   1.a, 1.b, 1.d and 1.e
//


//##############################################################################
//### Each of the following statements (`;` marks the end of a statement) 
//###   will be launched independently (one by one) in the Neo4j browser 
//##############################################################################


//## Clear the database: delete all existing nodes and relationships
MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r ;


//##############################################################################
//###	Create nodes associated with Romania's main regions (each is labeled "Region")
CREATE
	(Moldova:Region { regionName : 'Moldova'  }),
	(Muntenia:Region { regionName : 'Muntenia'  }),
	(Transilvania:Region { regionName : 'Transilvania'  }),
	(Banat:Region { regionName : 'Banat'  }) ;

//##	Declare uniqueness constraint for "Region"
CREATE CONSTRAINT ON (r:Region)
ASSERT r.regionName IS UNIQUE ;


//##############################################################################
//###	Create nodes associated with counties (each is labeled "County")
CREATE
	(Iasi_County:County { countyName : 'Iasi' }),
 	(Vaslui_County:County { countyName : 'Vaslui' }),
 	(Vrancea:County { countyName : 'Vrancea' }),
 	(Buzau_County:County { countyName : 'Buzau' }),
 	(Galati_County:County { countyName : 'Galati' }),
 	(Braila_County:County { countyName : 'Braila' }),
 	(Neamt:County { countyName : 'Neamt' }),
 	(Bacau_County:County { countyName : 'Bacau' }) 

//##	Declare uniqueness constraint for "County"
CREATE CONSTRAINT ON (c:County)
ASSERT c.countyName IS UNIQUE 



//##############################################################################
//###	Create relationships specifying each county's region
//###		each relation is unique (no risk of duplication

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Iasi' AND r.regionName = 'Moldova'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Vaslui' AND r.regionName = 'Moldova'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Vrancea' AND r.regionName = 'Moldova'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Buzau' AND r.regionName = 'Muntenia'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r)  ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Braila' AND r.regionName = 'Muntenia'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Galati' AND r.regionName = 'Moldova'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Neamt' AND r.regionName = 'Moldova'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;

MATCH (c:County),(r:Region)
WHERE c.countyName = 'Bacau' AND r.regionName = 'Moldova'
CREATE UNIQUE (c)-[rel:IS_IN_REGION]->(r) ;



//#######################################################################################
//###			 Create nodes associated with cities (each is labeled "City")
CREATE (Iasi:City { cityName:'Iasi', population: 290422 })
CREATE (Tg_Frumos:City { cityName:'Tirgu Frumos', population: 9386 })
CREATE (Vaslui:City { cityName:'Vaslui', population: 55407 })
CREATE (Barlad:City { cityName:'Barlad', population: 55837 })
CREATE (Tecuci:City { cityName:'Tecuci', population: 34871 })
CREATE (Tisita:City { cityName:'Tisita'})
CREATE (Focsani:City { cityName:'Focsani', population: 79315 })
CREATE (Rimnicu_Sarat:City { cityName:'Rimnicu Sarat', population: 33843 })
CREATE (Galati:City { cityName:'Galati', population: 249432 })
CREATE (Braila:City { cityName:'Braila', population: 180302 })
CREATE (Roman:City { cityName:'Roman', population: 50713 })
CREATE (Bacau:City { cityName:'Bacau', population: 144307 })
CREATE (Adjud:City { cityName:'Adjud', population: 16045 }) ;

//##	Declare uniqueness constraint for "City"
CREATE CONSTRAINT ON (c:City)
ASSERT c.cityName IS UNIQUE ;

//##	Declare an index for speeding the queries
CREATE INDEX ON :City(population) ;



//##############################################################################
//###	Create relationships specifying each city's county

MATCH (city:City),(county:County)
WHERE city.cityName IN ['Iasi', 'Tirgu Frumos'] AND county.countyName = 'Iasi'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName IN ['Vaslui', 'Barlad'] AND county.countyName = 'Vaslui'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName IN ['Tecuci', 'Galati'] AND county.countyName = 'Galati'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName IN ['Tisita', 'Focsani', 'Adjud'] AND county.countyName = 'Vrancea'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName = 'Rimnicu Sarat' AND county.countyName = 'Buzau'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName = 'Braila' AND county.countyName = 'Braila'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName = 'Roman' AND county.countyName = 'Neamt'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;

MATCH (city:City),(county:County)
WHERE city.cityName = 'Bacau' AND county.countyName = 'Bacau'
CREATE UNIQUE (city)-[rel:IS_IN_COUNTY]->(county) ;



//#######################################################################################
//### 		Create relations related to connections between cities
//###
//###  Next queries must be launched one by one in browser
//###   or go to directory "/usr/local/Cellar/neo4j/2.3.0/libexec/bin"
//###     and launch executable file "neo4j-shell"

//###  copy-paste all population queries below (all the queries with no RETURN option)

//# Iasi - Vaslui: 71 km
MATCH (from:City) WHERE from.cityName = 'Iasi'
MATCH (to:City) WHERE to.cityName = 'Vaslui'
WITH from, to CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 71}]-> (to) ;

//# Iasi - Tg.Frumos: 53km
MATCH (from:City) WHERE from.cityName = 'Iasi'
MATCH (to:City) WHERE to.cityName = 'Tirgu Frumos'
WITH from, to CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 53}]-> (to) ;

//# Tg.Frumos - Roman: 40
MATCH (from:City)
WHERE from.cityName = 'Tirgu Frumos'
MATCH (to:City)
WHERE to.cityName = 'Roman'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 40}]-> (to);

//# Roman - Bacau: 41;
MATCH (from:City)
WHERE from.cityName = 'Roman'
MATCH (to:City)
WHERE to.cityName = 'Bacau'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 41}]-> (to);

//# Roman - Vaslui: 80;
MATCH (from:City)
WHERE from.cityName = 'Roman'
MATCH (to:City)
WHERE to.cityName = 'Vaslui'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 80}]-> (to);

//# Vaslui - Barlad: 54;
MATCH (from:City)
WHERE from.cityName = 'Vaslui'
MATCH (to:City)
WHERE to.cityName = 'Barlad'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 54}]-> (to);

//# Barlad - Tecuci: 48;
MATCH (from:City)
WHERE from.cityName = 'Barlad'
MATCH (to:City)
WHERE to.cityName = 'Tecuci'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 48}]-> (to);

//# Tecuci - Tisita: 20;
MATCH (from:City)
WHERE from.cityName = 'Tecuci'
MATCH (to:City)
WHERE to.cityName = 'Tisita'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 20}]-> (to);

//# Tisita - Focsani: 13;
MATCH (from:City)
WHERE from.cityName = 'Tisita'
MATCH (to:City)
WHERE to.cityName = 'Focsani'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 13}]-> (to);

//# Bacau - Adjud: 60;
MATCH (from:City)
WHERE from.cityName = 'Bacau'
MATCH (to:City)
WHERE to.cityName = 'Adjud'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 60}]-> (to);

//# Bacau - Vaslui: 85;
MATCH (from:City)
WHERE from.cityName = 'Bacau'
MATCH (to:City)
WHERE to.cityName = 'Vaslui'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 85}]-> (to);

//# Adjud - Tisita: 32;
MATCH (from:City)
WHERE from.cityName = 'Adjud'
MATCH (to:City)
WHERE to.cityName = 'Tisita'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 32}]-> (to);

//# Focsani - Galati: 80;
MATCH (from:City)
WHERE from.cityName = 'Focsani'
MATCH (to:City)
WHERE to.cityName = 'Galati'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 80}]-> (to);

//# Focsani - Braila: 86;
MATCH (from:City)
WHERE from.cityName = 'Focsani'
MATCH (to:City)
WHERE to.cityName = 'Braila'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 86}]-> (to);

//# Focsani - Rm.Sarat: 42;
MATCH (from:City)
WHERE from.cityName = 'Focsani'
MATCH (to:City)
WHERE to.cityName = 'Rimnicu Sarat'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 42}]-> (to);

//# Rm.Sarat - Braila: 84;
MATCH (from:City)
WHERE from.cityName = 'Rimnicu Sarat'
MATCH (to:City)
WHERE to.cityName = 'Braila'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 84}]-> (to);

//# Braila - Galati: 11;
MATCH (from:City)
WHERE from.cityName = 'Braila'
MATCH (to:City)
WHERE to.cityName = 'Galati'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 11}]-> (to);

//# Tecuci - Galati, 67;
MATCH (from:City)
WHERE from.cityName = 'Tecuci'
MATCH (to:City)
WHERE to.cityName = 'Galati'
CREATE UNIQUE (from) -[r:CONNECTED_TO { distance: 67}]-> (to);

// if you want to delete a certain vertice (in this case the route leg between
//  `Galati` and `Tecuci`...
MATCH (from:City) -[r:CONNECTED_TO] -> (to:City)
WHERE from.cityName = 'Galati' AND to.cityName = 'Tecuci'
DELETE r
