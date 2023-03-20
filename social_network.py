from py2neo import Graph
graph = Graph("neo4j://localhost:7687", auth=("neo4j", "qwertyuiop"))

#clear the node
clear_cyp ='''MATCH(n)
DETACH DELETE n'''
graph.run(clear_cyp)

#load person data
cyp = '''load csv with  headers from "file:///dataset/sng_trips.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (p:Person {name:row.name})-[:OWN]-(d:Passport {passport_number:row.passportnumber})-[:CITIZEN_OF]-(c:Country{country:row.citizenship})'''

graph.run(cyp)

#load company data
comp_cyp ='''load csv with  headers from "file:///dataset/sng_work.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (p:Company {name_of_organization:row.nameoforganization, name:row.name})-[r:WORKING_AS]-(c:Position {designation:row.designation})-[:PERIOD_OF_SERVICES]-(d:Year{startyear:row.startyear, endyear:row.endyear})'''

graph.run(comp_cyp)

#load education data
edu_cyp = '''load csv with  headers from "file:///dataset/sng_education.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (p:Institution {name_of_institution:row.nameofinstitution, name:row.name})-[:Taking]-(c:Course{course: row.course})-[:STARTED_from]-(d:Study_Year{startyear:row.startyear, endyear:row.endyear})'''

graph.run(edu_cyp)


#load credit card data
cc_cyp = '''load csv with  headers from "file:///dataset/sng_transaction.csv" as row
WITH row WHERE row.passportnumber is not null
MATCH (p:Person{name:row.name})
SET p.cardnumber = row.cardnumber
MERGE (c:Credit_Card {cardnumber:row.cardnumber})'''

graph.run(cc_cyp)

#load transaction data
transaction_cyp = '''load csv with  headers from "file:///dataset/sng_transaction.csv" as row
WITH row WHERE row.passportnumber is not null
MATCH (cc:Credit_Card{cardnumber:row.cardnumber})
MERGE (cc)-[:MAKE]-(d:Transaction{transactiondate:row.transactiondate, merchant:row.merchant, amount:row.amount})'''

graph.run(transaction_cyp)

#load trip data
trip_cyp = '''load csv with  headers from "file:///dataset/sng_trips.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (t:Trip {passport_number:row.passportnumber, arrival_country:row.arrivalcountry, arrival_date:row.arrivaldate, departure_from:row.departurecountry, departure_date:row.departuredate})'''

graph.run(trip_cyp)

#link person to company
first_cyp = '''MATCH (p:Person),(c:Company)
               WHERE p.name = c.name
               REMOVE c.name
               MERGE (p)-[:WORKING_AT]-(c)'''
graph.run(first_cyp)

#link person to education
second_cyp = '''MATCH (p:Person),(i:Institution)
               WHERE p.name = i.name
               REMOVE i.name
               MERGE (p)-[:STUDY_AT]-(i)'''
graph.run(second_cyp)


#link person to credit card
third_cyp = '''MATCH (p:Person),(c:Credit_Card)
               WHERE p.cardnumber = c.cardnumber
               REMOVE p.cardnumber
               MERGE (p)-[:OWN]-(c)'''
graph.run(third_cyp)

#link person to trip
fourth_cyp = '''MATCH (p:Person),(pp:Passport),(t:Trip)
               WHERE pp.passport_number = t.passport_number
               MERGE (p)-[:WENT]-(t)'''
graph.run(fourth_cyp)