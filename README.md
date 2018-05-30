# data61paradise

Code to process csv files using spark and remove special keywords using spark and scala which can be used for neo4j data input

sbt run main is used to run this application which read data from csv filess which are stored at respective directories.


copy directories to import folder or other locations and give path for nodes and relatiionships

bin/neo4j-admin import  --nodes import/intermediary.csv/intermediary.csv --nodes import/others.csv/others.csv --nodes import/officer.csv/officer.csv --nodes import/entity.csv/entity.csv --relationships import/edges.csv/edges.csv --delimiter "," --quote "\"" --mode csv --database graph.db --ignore-missing-nodes true
