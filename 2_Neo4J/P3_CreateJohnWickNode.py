from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "katierocks"))
session = driver.session()
session.run("CREATE (m:Movie {title: {title}})",
	{"title": "John Wick"})

session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM {csv} AS row MERGE (a:Actor {name: row.name}) SET a.wickrole = row.wickrole",
	{"csv": "file:///C:/JohnWickCastList.csv"})

session.run("MATCH (a:Actor),(n:Movie {title : {title}}) WHERE a.wickrole IS NOT NULL CREATE(a)-[:ACTS_IN]->(n)",
	{"title": "John Wick"})

session.close()
