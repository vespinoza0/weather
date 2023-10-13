from flask import Flask
from flask import jsonify
import psycopg2
import pandas as pd
from flask_swagger_ui import get_swaggerui_blueprint


app = Flask(__name__)

# Database Connection. Returns psycopg2 connection object
def get_database_connection():
    db_creds = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost'  
    }
    connection = psycopg2.connect(**db_creds)
    return connection


# queries the database for weather stats
@app.route('/api/weather/stats', methods=['GET'])
def get_weather_stats():
    connection = get_database_connection()
    cursor = connection.cursor()
    try:
        qry = '''select site, "year",
	                avg(max_temp)/10 as avg_max_temp,
	                avg(min_temp)/10 as avg_min_temp,
	                sum(precipitation)/10 as annual_precipitation
                from my_table 
                    where max_temp != -9999 or min_temp != -9999 or precipitation!= -9999
                    group by site, "year"
                order by site desc, "year" asc 
        '''
        qry = '''select * from weather_stats'''
        data = pd.read_sql(qry, connection)
        data = data.to_json(orient="split")
        cursor.close()
        connection.close()
        return data 
    except Exception as e:
        # Handle errors, log, and return an error response
        return jsonify({'error': str(e)}), 500

# queries the database for all weather data
@app.route('/api/weather', methods=['GET'])
def get_weather_data():
    connection = get_database_connection()
    cursor = connection.cursor()
    try:
        data = pd.read_sql("select * from weather_data", connection)
        data = data.to_json(orient="split")
        cursor.close()
        connection.close()
        return data
    except Exception as e:
        # Handle errors, log, and return an error response
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)