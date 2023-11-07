from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import json
import pandas as pd

class connect_to_cluster :
#A class to connect to cassandra cluster in astra db  
   def __init__(self,secure_connect_path,token_path):
      self.__secure_connect_path = secure_connect_path
      self.__token_path = token_path

   def connect (self) :

      cloud_config= {
                      'secure_connect_bundle': self.__secure_connect_path
                    }
      with open(self.__token_path) as f:
           secrets = json.load(f)
      CLIENT_ID = secrets["clientId"]
      CLIENT_SECRET = secrets["secret"]

      auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
      cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
      session = cluster.connect()
      return(session)

def json_to_df (json_path):
    #open and load json file 
    with open(json_path) as data_file:    
         data_dicts = json.load(data_file)  

    #flatten json file to read as a data frame
    columns_df = pd.json_normalize(data_dicts['meta']['view']['columns']) 
    columns_names = columns_df["name"].to_list()
    df = pd.DataFrame(data_dicts['data'],columns = columns_names)
    return (df)



