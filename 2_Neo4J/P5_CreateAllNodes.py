from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "katierocks"))
session = driver.session()

#Import The Matrix
session.run("CREATE (m:Movie {title: {title}})", #Create the Movie Node
	{"title": "The Matrix"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.role = row.role",
	{"csv": "file:///TheMatrixCastList_Exp.csv"}) #Read the Actor names and roles from the .csv file
 
session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.role IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "The Matrix"})  #Create a relationship between the actors and the movie

#Import The Matrix Reloaded
session.run("CREATE (m:Movie {title: {title}})",	
	{"title": "The Matrix Reloaded"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.role = row.role",
	{"csv": "file:///TheMatrixCastList_Exp.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.role IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "The Matrix Reloaded"})

#Import The Matrix Reloaded
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "The Matrix Revolutions"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.role = row.role",
	{"csv": "file:///TheMatrixCastList_Exp.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.role IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "The Matrix Revolutions"})

#Import John Wick
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "John Wick"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.wickrole = row.wickrole",
	{"csv": "file:///JohnWickCastList_Exp.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.wickrole IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "John Wick"})

#Import Speed
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "Speed"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.speedrole = row.speedrole",
	{"csv": "file:///SpeedCastList_Exp.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.speedrole IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "Speed"})

#Import the Lake House
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "The Lake House"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.lakerole = row.lakerole",
	{"csv": "file:///LakeHouseCastList_Exp.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.lakerole IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "The Lake House"})

#Import the Devil's Advocate
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "The Devil's Advocate"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.devilrole = row.devilrole",
	{"csv": "file:///C:/DevilsAdvocateCastList_Exp.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.devilrole IS NOT NULL MERGE (a)-[:ACTS_IN]->(n)",
	{"title": "The Devil's Advocate"})

#Import Directors
session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MATCH(m:Movie{title:row.movie}) MERGE(d:Director { name: row.name }) MERGE(d)-[:DIRECTED]->(m)",
	{"csv": "file:///DirectorList_Exp.csv"})

session.close()