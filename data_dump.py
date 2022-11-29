import pymongo
import pandas as pd
import json
import os

#provide the mongo db local host url to connect python to mongodb.

client=pymongo.MongoClient(os.getenv("MONGO_DB_URL"))

DATA_FILE_PATH="C:\Mehul\\Data Scientist\\Machine Learning\\Projects\\sensor_fault_detection\\sensor\\data\\aps_failure_training_set1.csv"
DATABASE_NAME="aps"
COLLECTION_NAME="sensor"



if __name__=="__main__":
    df=pd.read_csv(DATA_FILE_PATH)
    print(f"Rows & Columns : {df.shape}")



    #convert dataframe to json so that we can dump these record in mongodb
    df.reset_index(drop=True,inplace=True)

    json_record = list(json.loads(df.T.to_json()).values())
    print(json_record[0])

    #insert converted json record to mongo db
    client[DATABASE_NAME][COLLECTION_NAME].insert_many(json_record)