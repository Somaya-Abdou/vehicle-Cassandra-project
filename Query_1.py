import pandas as pd
import connect_database as cd
from connect_database import connect_to_cluster

def Query_1 (df,session):
#We want to count Electric Vehicle Type 'Plug-in Hybrid Electric Vehicle (PHEV)' for each state

    #create table count_vehicle
    query = "Create table  if not exists vehicle.count_vehicle"
    query = query + "(id uuid ,\
                      state text,\
                      city text,\
                      electric_vehehicle_type text,\
                      Primary Key (electric_vehehicle_type,id))"
    session.execute(query)

    #insert into count_vehicle table
    for row_1,row_2,row_3 in zip (df['State'],df['City'],df['Electric Vehicle Type']) :
        query = "insert into vehicle.count_vehicle (id,state,city,electric_vehehicle_type)"
        query = query + "values (uuid(),%s,%s,%s)"
        session.execute(query,(row_1,row_2,row_3))

    #get count of veicle type PHEV    
    query =("select state ,electric_vehehicle_type, count(electric_vehehicle_type) as count_of_vehicles_types\
             from vehicle.count_vehicle where electric_vehehicle_type = 'Plug-in Hybrid Electric Vehicle (PHEV)' ")
    rows =  session.execute(query)
    for row in rows :
        print(row)

def main():    
    secure_connect_path = 'secure-connect-tutorial.zip'
    token_path = "tutorial-token.json"
    json_path = 'Electric_Vehicle_Population_Data.json'

    df = cd.json_to_df(json_path)
    session = connect_to_cluster(secure_connect_path,token_path).connect()
    Query_1 (df,session)

main()