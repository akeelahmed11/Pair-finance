from ast import Continue
from cmath import e
import math
from multiprocessing.connection import wait
from os import environ
from pickle import APPEND
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy import Table, Column, Integer, String, MetaData, Float
import sqlalchemy as db
import pandas as pd
import numpy as np
import re, math
from datetime import datetime, timedelta
from pandas.core.ops.array_ops import Timestamp
import time as pyTime


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')


while True:
    try:
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine.execute("DROP table IF EXISTS devices_agg")
        break
    except OperationalError:
        sleep(0.1)
print('Connection to MySQL successful.')





# Write the solution here
connection = psql_engine.connect()
metadata = db.MetaData()
census = db.Table('devices', metadata, autoload=True, autoload_with=psql_engine)

index = 0
df = pd.DataFrame([], columns=["Device","Temperature","Location","Timestamp","Time","Date","Hour","Lat","Long", "Distance"])

def get_lat(s):
       return float(re.findall('latitude": "(.+)", "', s)[0])


def get_long(s):
       return float(re.findall('longitude": "(.+)"', s)[0])

hourly = False

while True:

    query = db.select([census]) 
    ResultProxy = connection.execute(query)
    ResultSet = ResultProxy.fetchall()

    
    df1 = pd.DataFrame(ResultSet[index:], columns=["Device","Temperature","Location","Timestamp"])
    df1["Time"] = pd.to_datetime(df1['Timestamp'],unit='s')
    df1['Date'] = pd.to_datetime(df1['Time']).dt.normalize()
    df1['Hour'] = df1['Time'].dt.strftime("%H")



    df1 = df1.astype({'Location':'string'})
    df1['Lat'] = df1['Location'].apply(lambda x: get_lat(x))
    df1['Long'] = df1['Location'].apply(lambda x: get_long(x))
    df1 = df1.sort_values(by = ['Device', 'Timestamp'], ascending = [True, True] ).reset_index(drop=True)

    
    df1['Distance'] = 0

    for i in df1.index[:-1]:
        
        df1.loc[i+1, 'Distance'] = math.sqrt((df1.loc[i+1, 'Lat'] - df1.loc[i, 'Lat'])**2 + (df1.loc[i+1, 'Long'] - df1.loc[i, 'Long'])**2)
        if df1.loc[i, 'Device'] != df1.loc[i+1, 'Device']:
                  df1.loc[i+1, 'Distance'] = 0
        

    if df.shape[1] == df1.shape[1]:
        data = [df, df1]
        df = pd.concat(data)  


    time = datetime.now()
    Date = time.strftime('%Y-%m-%d')
    this_hour = str(time.hour)
    prev_hour = str(time.hour - 1)

    df.drop_duplicates(keep='first', inplace=True)
    df.reset_index(drop=True)


    print("Time now: ",time)
    if hourly == False:
       
        data = df[(df["Date"].dt.strftime('%Y-%m-%d') <= Date) & (df["Hour"] < this_hour)]
        print("All Time Data - Shape: ", data.shape)
        hourly = True

    elif hourly == True:

        print("Current Hour Data Shape: ",df[(df["Date"].dt.strftime('%Y-%m-%d') == Date) & (df["Hour"] >= this_hour)].shape, "\n")
        if datetime.now().minute == 1:
            
            data = df[(df["Date"].dt.strftime('%Y-%m-%d') == Date) & (df["Hour"] == prev_hour)]
            print("Writing Previous Hourly Data to DB: ", data.shape)
            pyTime.sleep(60)

    
    try:

        Grouped_data = pd.DataFrame(data.groupby(['Device','Date', 'Hour']).agg({"Temperature" : ["max"], "Device": ["count"], "Distance" :  ["sum"]}).reset_index())
        Grouped_data.columns = ["Device","Date","Hour","Max_Temperature","Device_Records","Distance_Covered"]
        Grouped_data = Grouped_data.sort_values(by = ['Date', 'Hour'], ascending = [False, False] ).reset_index(drop=True)
        print(Grouped_data, "\n")


        # Adding Data to Sql

        Grouped_data.to_sql('devices_agg', mysql_engine, if_exists='append', dtype={"Device": String(300), "Date": String(30), "Hour": String(30), "Max_Temperature": Integer(), "Device_Records": Integer(), "Distance_Covered": Float()}, index=False)
        meta_data = db.MetaData(bind=mysql_engine)
        db.MetaData.reflect(meta_data)
        
        # GET THE `actor` TABLE FROM THE METADATA OBJECT
        actor_table = meta_data.tables['devices_agg']
        
        # SELECT COUNT(*) FROM Actor
        result = db.select([db.func.count()]).select_from(actor_table).scalar() 
        print("Sql Table Count:", result)


    except :
        print(" ")
        continue

    index = len(ResultSet) 
  