--############################################################################
--### 	      Export `chinook` database from PostgreSQL to Neo4j - 
--###    in this new version the composers are transformed into arrays
--#############################################################################
-- last update: 2025-05-06

-- I.
-- Connect to PgAdmin
-- create and populate database `chinook` using script
-- `neo-04a2 Chinook_PostgreSql 2017.sql`

-- set `chinook` as the current/default database in pgAdmin IV, then run the following statements:

-- this is a PL/pgSQL function for converting the attribute `track.composer` from string into array
--- (based on the separators between composers)
CREATE OR REPLACE FUNCTION f_track_composers (composer_init VARCHAR) 
  RETURNS TEXT AS $f_track_composers$
DECLARE
	the_array TEXT[] := ARRAY[]::TEXT[];
	array_as_string TEXT ;
BEGIN
	the_array := regexp_split_to_array(composer_init, '/|, |,|;|&| - | -') ;
	WITH ds AS (
		SELECT LTRIM(RTRIM(composer)) AS composer
		FROM unnest(the_array) AS composer)
	SELECT string_agg(composer, '", "' )
	INTO array_as_string 
	FROM ds ;
 
 	RETURN '["' || REGEXP_REPLACE(array_as_string, $$''$$, $$'$$ ) || '"]' ;

END ; 
$f_track_composers$ LANGUAGE plpgsql ;


			   
-- SELECT f_track_composers('Angus Young, Malcolm Young, Brian Johnson')
-- SELECT f_track_composers('F. Baltes, S. Kaufman, U. Dirkscneider & W. Hoffman')
-- SELECT f_track_composers('Deaffy & R.A. Smith-Diesel')
-- SELECT f_track_composers('Billie Joe Armstrong -Words Green Day -Music')
SELECT f_track_composers($$Billie Joe Armstrong's -Words Green Day''s -Music$$)


DROP TABLE IF EXISTS chinook__pg_to_neo4j ;

CREATE TABLE chinook__pg_to_neo4j AS
SELECT text FROM (
			SELECT * FROM (

	-- 1. Create `:Artist` nodes
   SELECT DISTINCT 1 AS order_in_result,
   		'CREATE (artist_' || artistid || ':Artist {name:"' || name || '", artistid:' || artistid ||'})'
   		AS text
   	FROM (SELECT * FROM artist ORDER BY artistid ) artist

	UNION

	-- 2. Include a statement separator (`;`)
  SELECT 2 AS order_in_result,
   		';' AS text

	UNION

	-- 3. Create `:Album` nodes
  SELECT 3 AS order_in_result,
   		'CREATE (album_'|| albumid ||':Album {title:"' || title || '", albumid:' ||
					albumid || '}) ' AS text
  FROM (SELECT * FROM album ORDER BY title ) album

	UNION

	-- 4. Include a statement separator (`;`)
  SELECT 4 AS order_in_result,
   		';' AS text

	UNION

	-- 5. Create relationships between `:Album` nodes and `:Artist` nodes
  SELECT 5 AS order_in_result,
   		'MATCH (al:Album), (ar:Artist) WHERE al.albumid = ' || albumid ||
			' AND ar.artistid = ' || artistid || ' MERGE (al) -[:wasReleasedBy]-> (ar) ;'
   		AS text
  FROM (SELECT * FROM album ORDER BY title ) album

	UNION

	-- 6. Create `:Track` nodes, including tracks media and genre
  SELECT 6 AS order_in_result,
   		'CREATE (track_' || trackid  || ':Track {track_name:"' || track.name ||
					'", trackid:' || trackid ||
				CASE WHEN mediatype.name IS NOT NULL
						THEN ', mediatype:"' || mediatype.name || '"' ELSE '' END ||
				CASE WHEN genre.name IS NOT NULL
						THEN ', genre:"' || genre.name || '"' ELSE '' END ||
--				CASE WHEN composer IS NOT NULL
--						THEN ', composer:"' || composer || '"' ELSE '' END ||
				CASE WHEN composer IS NOT NULL
						THEN ', composer: ' || f_track_composers(composer) || '' ELSE '' END ||
				CASE WHEN milliseconds IS NOT NULL
						THEN ', milliseconds:' || milliseconds  ELSE '' END ||
				CASE WHEN bytes IS NOT NULL
						THEN ', bytes:' || bytes  ELSE '' END ||
				CASE WHEN unitprice IS NOT NULL
						THEN ', unitprice:' || unitprice  ELSE '' END ||
						'}) ;'
   		AS text
	FROM track
		 	LEFT JOIN mediatype ON track.mediatypeid = mediatype.mediatypeid
			LEFT JOIN genre ON track.genreid = genre.genreid

	UNION

	-- 7. Create relationships between `:Track` nodes and `:Album` nodes
  SELECT 7 AS order_in_result,
   		'MATCH (t:Track), (a:Album) WHERE t.trackid = ' || trackid ||
			' AND a.albumid = ' || albumid || ' MERGE (t) -[:includedOn]-> (a) ;'
   		AS text
  FROM (SELECT * FROM track ORDER BY name ) track

	UNION

	-- 8. Create `:Playlist` nodes
  SELECT DISTINCT 8 AS order_in_result,
   		'CREATE (playlist_' || playlistid || ':Playlist {playlist_name:"' || name ||
				'", playlistid:' || playlistid || '})'
   		AS text
  FROM playlist

	UNION

	-- 9. Include a statement separator (`;`)
  SELECT 9 AS order_in_result,
   		';' AS text

	UNION

	-- 10. Link `:Track` nodes to `:Playlist` nodes
	SELECT 10 AS order_in_result,
		'MATCH (t:Track), (p:Playlist) WHERE t.trackid = ' || trackid ||
		' AND p.playlistid = ' || playlistid || ' MERGE (t) -[:includedIn]-> (p) ;'
			AS text
	FROM playlisttrack

	UNION

	-- 11. Create `:Employee` nodes (without `ReportsTo` property)
  SELECT DISTINCT 11 AS order_in_result,
   			'CREATE (:Employee {lastname:"' || lastname || '", firstname:"' ||
				firstname || '", employeeid:' || employeeid ||
				CASE WHEN title IS NOT NULL
						THEN ', title:"' || title || '"' ELSE '' END ||
				CASE WHEN birthdate IS NOT NULL
						THEN ', birthdate: date("' || DATE(birthdate) || '")' ELSE '' END ||
				CASE WHEN hiredate IS NOT NULL
						THEN ', hiredate: date("' || DATE(hiredate) || '")' ELSE '' END ||
				CASE WHEN address IS NOT NULL
						THEN ', address:"' || address || '"' ELSE '' END ||
				CASE WHEN city IS NOT NULL
						THEN ', city:"' || city || '"' ELSE '' END ||
				CASE WHEN state IS NOT NULL
						THEN ', state:"' || state || '"' ELSE '' END ||
				CASE WHEN country IS NOT NULL
						THEN ', country:"' || country || '"' ELSE '' END ||
				CASE WHEN postalcode IS NOT NULL
						THEN ', postalcode:"' || postalcode || '"' ELSE '' END ||
				CASE WHEN phone IS NOT NULL
						THEN ', phone:"' || phone || '"' ELSE '' END ||
				CASE WHEN fax IS NOT NULL
						THEN ', fax:"' || fax || '"' ELSE '' END ||
				CASE WHEN email IS NOT NULL
						THEN ', email:"' || email || '"' ELSE '' END ||
						'}) ;'
   		AS text
  FROM Employee

	UNION

	-- 12. Link every employee to her/his boss
	SELECT 12 AS order_in_result,
		'MATCH (sub:Employee), (boss:Employee) WHERE sub.employeeid = ' || employeeid ||
		' AND boss.employeeid = ' || ReportsTo || ' MERGE (sub) -[:isSubordinatedTo]-> (boss) ;'
			AS text
	FROM Employee
	WHERE ReportsTo IS NOT NULL

	UNION

	-- 13. Create `:Customer` nodes
  SELECT DISTINCT 13 AS order_in_result,
   			'CREATE (:Customer {lastname:"' || lastname || '", firstname:"' ||
				firstname || '", customerid:' || customerid ||
				CASE WHEN company IS NOT NULL
						THEN ', company:"' || company || '"' ELSE '' END ||
				CASE WHEN address IS NOT NULL
						THEN ', address:"' || address || '"' ELSE '' END ||
				CASE WHEN city IS NOT NULL
						THEN ', city:"' || city || '"' ELSE '' END ||
				CASE WHEN state IS NOT NULL
						THEN ', state:"' || state || '"' ELSE '' END ||
				CASE WHEN country IS NOT NULL
						THEN ', country:"' || country || '"' ELSE '' END ||
				CASE WHEN postalcode IS NOT NULL
						THEN ', postalcode:"' || postalcode || '"' ELSE '' END ||
				CASE WHEN phone IS NOT NULL
						THEN ', phone:"' || phone || '"' ELSE '' END ||
				CASE WHEN fax IS NOT NULL
						THEN ', fax:"' || fax || '"' ELSE '' END ||
				CASE WHEN email IS NOT NULL
						THEN ', email:"' || email || '"' ELSE '' END ||
						'}) ;'
   		AS text
  FROM customer

	UNION

	-- 14. Link `:Customer` nodes to `:Employee` nodes
	SELECT 14 AS order_in_result,
		'MATCH (c:Customer), (e:Employee) WHERE c.customerid = ' || customerid ||
		' AND e.employeeid = ' || supportrepid || ' MERGE (c) -[:addressedForSupportTo]-> (e) ;'
			AS text
	FROM customer
	WHERE supportrepid IS NOT NULL

	UNION

	-- 15. Create `:Invoice` nodes
  SELECT DISTINCT 15 AS order_in_result,
   			'CREATE (:Invoice {invoiceid:' || invoiceid ||
				CASE WHEN invoicedate IS NOT NULL
						THEN ', invoicedate: date("' || DATE(invoicedate) || '")' ELSE '' END ||
				CASE WHEN billingaddress IS NOT NULL
						THEN ', billingaddress:"' || billingaddress || '"' ELSE '' END ||
				CASE WHEN billingcity IS NOT NULL
						THEN ', billingcity:"' || billingcity || '"' ELSE '' END ||
				CASE WHEN billingstate IS NOT NULL
						THEN ', billingstate:"' || billingstate || '"' ELSE '' END ||
				CASE WHEN billingcountry IS NOT NULL
						THEN ', billingcountry:"' || billingcountry || '"' ELSE '' END ||
				CASE WHEN billingpostalcode IS NOT NULL
						THEN ', billingpostalcode:"' || billingpostalcode || '"' ELSE '' END ||
				CASE WHEN total IS NOT NULL
						THEN ', total:' || total ELSE '' END ||
						'}) ;'
   		AS text
  FROM invoice

	UNION

	-- 16. Link `:Invoice` nodes to `:Customer` nodes through ':wasBought' relationships
	SELECT DISTINCT 16 AS order_in_result,
		'MATCH (i:Invoice), (c:Customer) WHERE i.invoiceid = ' || invoiceid ||
		' AND c.customerid = ' || customerid || ' MERGE (i) -[:wasBought]-> (c) ;'
			AS text
	FROM invoice
	WHERE customerid IS NOT NULL

	UNION

	-- 17. Link `:Invoice` nodes to `:Track` nodes through ':Contains' relationships
	-- these relationships contain attributes taken from table `invoiceline`
	SELECT DISTINCT 17 AS order_in_result,
		'MATCH (i:Invoice), (t:Track) WHERE i.invoiceid = ' || invoiceid ||
		' AND t.trackid = ' || trackid || ' MERGE (i) -[:Contains {' ||
		'invoicelineid:' || invoicelineid ||
		CASE WHEN unitprice IS NOT NULL
						THEN ', unitprice:' || unitprice ELSE '' END ||
		CASE WHEN quantity IS NOT NULL
						THEN ', quantity:' || quantity ELSE '' END ||
		'}]-> (t) ;'
			AS text
	FROM invoiceline

			) x
			ORDER BY 1, 2
		) y
	;

SELECT * FROM chinook__pg_to_neo4j ;



/*

--############################################################################
-- II. Next, in PgAdmin,
	2.1 click on the table `chinook__pg_to_neo4j`
	2.2 righ-click, and choose `Import/Export`, then 'Export`
	2.3 choose `Format` as `text` and save it a accessibile directory

The result must ressemble to the content of the file
`neo-04c_chinook_pg_to_neo4j.cypher`


-- #####################################################################################################
-- III. Create the `chinook` database in Neo4j  
 	3.1. Launch Neo4j Desktop
 	3.2. Create a project called `chinook`
	3.3. Create a database (Local Graph) called `chinook` (pay attention to the password)
	3.4. Start (make active) the database `chinook`

-- #####################################################################################################
-- IV. Run the Cypher (.cql) script `neo-04c_chinook_pg_to_neo4j.cypher` with Cypher Shell 
	4.1. Download Cypher Shell from `https://neo4j.com/download-center/`
	4.2. Install (copy) it into local (accesibile) directory (e.g. D:/CypherShell)
	4.3. Copy the script `neo-04c_chinook_pg_to_neo4j.cypher` (from the step 2.3) into local CypherShell directory
	4.4 Launch Windows/Mac Terminal Mode (Command Prompt)
	4.5. Set the CypherShell directory as the current directory (`cd D:/CypherShell`)
	4.6 In Terminal/Command Prompt launch the script with Cypher Shell:
	 - on Mac: 
	 	./cypher-shell -u neo4j -p chinook2023  -d neo4j  -f neo-04c_chinook_pg_to_neo4j.cypher
	 - on Windows:	
	 	cypher-shell -u neo4j -p chinook2023  -d neo4j  -f neo-04c_chinook_pg_to_neo4j.cypher
*/
