from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "katierocks"))
session = driver.session()
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "The Devil's Advocate"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.devilrole = row.devilrole",
	{"csv": "file:///C:/DevilsAdvocateCastList.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.devilrole IS NOT NULL CREATE (a)-[:ACTS_IN]->(n)",
	{"title": "The Devil's Advocate"})

session.close()