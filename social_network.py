from py2neo import Graph
graph = Graph("neo4j://localhost:7687", auth=("neo4j", "qwertyuiop"))

#clear the node
clear_cyp ='''MATCH(n)
DETACH DELETE n'''
graph.run(clear_cyp)

#load Country data
country_cyp = '''load csv with  headers from "file:///dataset/sng_transaction.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (c:Country{country:row.country})'''

graph.run(country_cyp)

#load person data
cyp = '''load csv with  headers from "file:///dataset/sng_trips.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (p:Person {name:row.name, passport_number:row.passportnumber, country:row.citizenship})-[:OWN]-(d:Passport {passport_number:row.passportnumber})'''

graph.run(cyp)

#load company data
comp_cyp ='''load csv with  headers from "file:///dataset/sng_work.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (p:Company {name_of_organization:row.nameoforganization, passport_number:row.passportnumber, country:row.country})-[r:WORKING_AS]-(c:Position {designation:row.designation})-[:PERIOD_OF_SERVICES]-(d:Year{startyear:row.startyear, endyear:row.endyear})'''

graph.run(comp_cyp)

#load education data
edu_cyp = '''load csv with  headers from "file:///dataset/sng_education.csv" as row
WITH row WHERE row.passportnumber is not null
MERGE (p:Institution {name_of_institution:row.nameofinstitution, passport_number:row.passportnumber, country:row.country})-[:Taking]-(c:Course{course: row.course})-[:STARTED_from]-(d:Study_Year{startyear:row.startyear, endyear:row.endyear})'''

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
               WHERE p.passport_number = c.passport_number
               REMOVE c.passport_number
               MERGE (p)-[:WORKING_AT]-(c)'''
graph.run(first_cyp)

#link Person to country
second_cyp = '''MATCH (p:Person), (ct:Country)
               WHERE p.country = ct.country
               REMOVE p.country
               MERGE (p)-[:CITIZEN_OF]-(ct)'''
graph.run(second_cyp)

#link Company to country
second_cyp = '''MATCH (c:Company), (ct:Country)
               WHERE c.country = ct.country
               REMOVE c.country
               MERGE (c)-[:IN]-(ct)'''
graph.run(second_cyp)

#link person to education
third_cyp = '''MATCH (i:Institution), (p:Person)
               WHERE i.passport_number  = p.passport_number
               REMOVE i.passport_number
               MERGE (p)-[:STUDY_AT]-(i)'''
graph.run(third_cyp)

#link Education to Country
fourth_cyp = '''MATCH (i:Institution),(ct:Country)
               WHERE i.country  = ct.country 
               REMOVE i.country
               MERGE (i)-[:IN]-(ct)'''
graph.run(fourth_cyp)

#link person to credit card
fifth_cyp = '''MATCH (p:Person),(c:Credit_Card)
               WHERE p.cardnumber = c.cardnumber
               REMOVE p.cardnumber
               MERGE (p)-[:OWN]-(c)'''
graph.run(fifth_cyp)

#link Person to Trip
six_cyp = '''MATCH (p:Person),(c:Trip)
               WHERE p.passport_number = c.passport_number
               REMOVE p.passport_number
               MERGE (p)-[:WENT_TO]-(c)'''
graph.run(six_cyp)

#link Trip to Country
seven_cyp = '''MATCH (p:Trip),(c:Country)
               WHERE p.departure_from = c.country
               REMOVE p.departure_from
               MERGE (p)-[:DEPATURE_FROM]-(c)'''
graph.run(seven_cyp)