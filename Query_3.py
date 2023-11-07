import pandas as pd
import connect_database as cd
from connect_database import connect_to_cluster

def Query_3 (df,session):
 #getting count of Clean Alternative Fuel Vehicle (CAFV) Eligibility for specific cars  
    query = "drop table if exists vehicle.eligibility"
    session.execute(query)
    
    #create table eligibility
    query = "Create table if not exists vehicle.eligibility"
    query = query + "(id uuid,\
                      model_county list<text>,\
                      make text,\
                      cafv_eligibility text,\
                      primary key(make,cafv_eligibility,id))"
    session.execute(query)
    
    #insert into eligibility table
    for row_1,row_2,row_3,row_4 in zip \
        (df['Model'],df['Make'],df['Clean Alternative Fuel Vehicle (CAFV) Eligibility'],df['County']) :
        query = "insert into vehicle.eligibility (id, model_county, make, cafv_eligibility)"
        query = query + f"values (uuid(),['{row_1}','{row_4}'],'{row_2}','{row_3}')"
        session.execute(query)
    
    #getting count of Clean Alternative Fuel Vehicle (CAFV) Eligibility for BMW cars
    query = "select count(cafv_eligibility) as count_cafv_eligibility ,model_county,make from vehicle.eligibility\
             where make = 'BMW' AND cafv_eligibility = 'Clean Alternative Fuel Vehicle Eligible'  "    
    rows = session.execute(query)
    for row in rows :
        print(row)

    
    #getting count of Eligibility unknown as battery range has not been researched for BMW cars
    query = "select count(cafv_eligibility) as count_cafv_unknown_eligibility ,model_county,make from vehicle.eligibility\
             where make = 'BMW' AND cafv_eligibility = 'Eligibility unknown as battery range has not been researched'  "    
    rows = session.execute(query)
    for row in rows :
        print(row)    
    
def main():    
    secure_connect_path = 'secure-connect-tutorial.zip'
    token_path = "tutorial-token.json"
    json_path = 'Electric_Vehicle_Population_Data.json'

    df = cd.json_to_df(json_path)
    session = connect_to_cluster(secure_connect_path,token_path).connect()
    Query_3 (df,session)

main()