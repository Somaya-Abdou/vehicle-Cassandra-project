# vehicle-Cassandra-project
In this project a large java vehicle file is being loaded into a Cassandra data frame then tables with interesting methods 
such as user data type UDT, sets, lists, and frozen collections are created to query the data.

Tables are created into the keyspace vehicle into ASTRA database.

First open https://astra.datastax.com , craete a database and then craete a keyspace .
Then download bundle and generate token from connect and put them in the folder of the project.
Jave file https://drive.google.com/file/d/19pBnPYfOjoo-mRLAgat8NjC6KJUU1N-R/view?usp=sharing

connect_database contains class method to connect to astra database and a function to flatten json file and open
as a dataframe

Query_1,Query_2,Query_3 are the query files to create tables and get select results

# libraries used
cassandra.cluster,

cassandra.auth,

json,

pandas,
