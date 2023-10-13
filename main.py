import os
import pandas as pd
import numpy as np
import datetime as dt
from sqlalchemy import create_engine
import logging


logging.basicConfig(filename='main.log',format='%(asctime)s:%(levelname)s:%(message)s',
                datefmt='%m-%d-%Y %I:%M:%S %p', level=logging.INFO)

logging.info("starting data ingestion ")
# --------------------- Problem 2 - Ingestion
# list of files in wx_data directory
files = os.listdir('./wx_data')
#data from first file in wx_data directory
data = pd.read_csv('./wx_data/'+files[0],names=["date", "max_temp", "min_temp", "precipitation"], delimiter='\t')
# add 'site' column to 
data['site'] = files[0].split('.')[0]

start= dt.datetime.now()
# concat all data in wx_data directory file into one dataframe 
for file in files[1:]:
    newdata = pd.read_csv('./wx_data/'+file, index_col=None,
            names=["date", "max_temp", "min_temp", "precipitation"],delimiter='\t')
    newdata['site'] = file.split('.')[0]
    data = pd.concat([data,newdata]) 

# replace -9999 as nan so that they register as null in postgres. Postgres ignores NULLs when using AVG()
data.replace(-9999, np.nan, inplace=True)
# convert date column to datetime.date type
data['date']= pd.to_datetime(data['date'], format= '%Y%m%d')
# extract year from date column and add 'year' column
data['year'] = data['date'].astype(str).str.slice(stop=4)
# database credentials
database = {
        'host':'localhost',
        'username' : 'postgres',
        'password' : 'postgres',
        'database' : 'postgres'
        }
# Write the dataframe to a postgresql DB 
engine = create_engine('postgresql://{username}:{password}@{host}:5432/{database}'.format(
                    username=database.get('username'),
                    password=database.get('password'),
                    host=database.get('host'),
                    database=database.get('database')
                    ))
# write to postgres table 'weather_data'
data.to_sql('weather_data',engine, if_exists='replace', index=False)
logging.info("finished data ingestion")
logging.info("wrote {} rows to weather_data table".format(len(data)))



#---------------------- Problem 3 - Data Analysis
# query to calculate 
# Average maximum temperature (in degrees Celsius)
# Average minimum temperature (in degrees Celsius)
# Total accumulated precipitation (in centimeters)
qry = '''select site, "year",
	                avg(max_temp)/10 as avg_max_temp,
	                avg(min_temp)/10 as avg_min_temp,
	                sum(precipitation)/10 as annual_precipitation
                from my_table 
                    where max_temp != -9999 or min_temp != -9999 or precipitation!= -9999
                    group by site, "year"
                order by site desc, "year" asc '''
# get stats from weather_data table using query
stats = pd.read_sql(qry, engine)
#write stats to 'weather_stats' table
stats.to_sql('weather_stats',engine, if_exists='replace', index=False)
