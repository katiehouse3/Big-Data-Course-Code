from neo4j.v1 import GraphDatabase

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "katierocks"))
session = driver.session()

#John Wick
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'Chad Stahelski' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "John Wick"})
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'David Leitch' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "John Wick"})

#Speed
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'Jan de Bont' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "Speed"})

#The Devil's Advocate
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'Taylor Hackford' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "The Devil's Advocate"})

#The Lake House
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'Alejandro Agresti' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "The Lake House"})

#The Matrix
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'Lana Wachowski' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "The Matrix"})
session.run("MATCH(m:Movie{title:{title}}) MERGE(d:Director { name:'Lilly Wachowski ' }) MERGE(d)-[:DIRECTED]->(m)",
	{"title": "The Matrix"})

session.close()