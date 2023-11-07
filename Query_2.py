import pandas as pd
import connect_database as cd
from connect_database import connect_to_cluster

def Query_2 (df,session):
 #getting average of electric range for specific year and city   
    query = "drop table if exists vehicle.range"
    session.execute(query)
    query = "drop Type if exists vehicle.data"
    session.execute(query)

    #create data type UDT data
    query = "Create Type if not exists vehicle.data"
    query = query + "(make text, model text , model_year int)"
    session.execute(query)
    
    #create table range
    query = "Create table if not exists vehicle.range"
    query = query + "(id uuid,\
                      details frozen <vehicle.data>,\
                      electric_range int,\
                      city text,\
                      model_year int,\
                      primary key(city,model_year,electric_range,id))"
    session.execute(query)
    
    #insert into vehicle table
    for row_1,row_2,row_3,row_4,row_5,row_6 in zip \
       (df['Make'],df['Model'],df['Model Year'],df['Electric Range'],df['City'],df['Model Year']) :
        query = "insert into vehicle.range (id, details, electric_range, city, model_year)"
        query = query + f"values (uuid(),{{make : '{row_1}' ,model : '{row_2}' ,model_year : {row_3}}},\
                                 {row_4},'{row_5}',{row_6})"
        session.execute(query)
    
    #getting average electric range of seattle cars in 2020
    query = "select city , avg(electric_range) as avg_electric_range , details from vehicle.range\
             where city = 'Seattle' AND model_year = 2020  "    
    rows = session.execute(query)
    for row in rows :
        print(row)
    
def main():    
    secure_connect_path = 'secure-connect-tutorial.zip'
    token_path = "tutorial-token.json"
    json_path = 'Electric_Vehicle_Population_Data.json'

    df = cd.json_to_df(json_path)
    session = connect_to_cluster(secure_connect_path,token_path).connect()
    Query_2 (df,session)

main()