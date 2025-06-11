import logging
import requests
import sys
import traceback
import datetime
import time
from configparser import ConfigParser
from urllib import parse
from requests.exceptions import ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError
import mysql.connector

BASE_WEATHER_API_URL = 'http://api.weatherapi.com/v1/'


def retry(func):
    def wrapper(*args, **kwargs):
        delay = 2
        for attempt in range(1, 4):
            try:
                return func(*args, **kwargs)
            except (ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError) as e:
                logging.warning(
                    f"[Retry {attempt}/3] {func.__name__} failed with: {e}. Retrying in {delay} seconds..."
                )
                time.sleep(delay)
                delay *= 2
        logging.error(f"All retries failed for {func.__name__}")
        return None
    return wrapper


class WeatherExtractor:
    def __init__(self):
        config = ConfigParser()
        config.read("secrets.ini")
        self.api_key = config["weather"]["api_key"]
        self.cities = [city.strip() for city in config["weather"]["cities"].split(',')]
        self.db_config = {
            'host': config["mysql"]["host"],
            'user': config["mysql"]["user"],
            'password': config["mysql"]["password"],
            'database': config["mysql"]["database"]
        }
        self.conn = mysql.connector.connect(**self.db_config)
        self.cursor = self.conn.cursor()
        self.create_table()
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    def create_table(self):
        query = """
        CREATE TABLE IF NOT EXISTS weather_records (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            location VARCHAR(100),
            country VARCHAR(100),
            min_temp FLOAT,
            max_temp FLOAT,
            humidity FLOAT,
            air_quality VARCHAR(50),
            type ENUM('FORECAST', 'HISTORY'),
            UNIQUE KEY unique_entry (date, location, type)
        );
        """
        self.cursor.execute(query)
        self.conn.commit()

    def exception_handling(self):
        ex_type, ex_value, ex_traceback = sys.exc_info()
        if ex_traceback is not None:
            trace_back = traceback.extract_tb(ex_traceback)
            stack_trace = [f"File : {trace[0]}, Line : {trace[1]}, Func.Name : {trace[2]}, Message : {trace[3]}" for trace in trace_back]
        else:
            stack_trace = ["No traceback available"]
        logging.error(f"Exception type : {ex_type.__name__}")
        logging.error(f"Exception message : {ex_value}")
        logging.error(f"Stack trace : {stack_trace}")

    @retry
    def fetch_data(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def insert_record(self, row):
        try:
            query = """
            INSERT INTO weather_records
            (date, location, country, min_temp, max_temp, humidity, air_quality, type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                min_temp = VALUES(min_temp),
                max_temp = VALUES(max_temp),
                humidity = VALUES(humidity),
                air_quality = VALUES(air_quality);
            """
            self.cursor.execute(query, (
                row['Date'],
                row['Location'],
                row['Country'],
                row['Min_Temp'],
                row['Max_Temp'],
                row['Humidity'],
                row['Air_Quality'],
                row['Type']
            ))
            self.conn.commit()

            if self.cursor.rowcount == 1:
                logging.info(f"Successfully wrote data for {row['Location']} on {row['Date']} ({row['Type']})")
            else:
                logging.info(f"Updated existing data for {row['Location']} on {row['Date']} ({row['Type']})")
        except mysql.connector.Error as err:
            logging.error(f"MySQL error: {err}")
            self.exception_handling()

    def extract_and_write(self, data, mode):
        try:
            forecast_days = data['forecast']['forecastday']
            for day_data in forecast_days:
                day = day_data['day']
                row = {
                    'Date': day_data['date'],
                    'Location': data['location']['name'],
                    'Country': data['location']['country'],
                    'Min_Temp': day['mintemp_c'],
                    'Max_Temp': day['maxtemp_c'],
                    'Humidity': day['avghumidity'],
                    'Air_Quality': data.get('current', {}).get('air_quality', {}).get('co', 'N/A'),
                    'Type': mode.upper()
                }
                self.insert_record(row)
        except Exception as e:
            logging.error(f"Error extracting/writing {mode} data: {e}")
            self.exception_handling()

    def build_forecast_url(self, city):
        params = {
            "q": city,
            "days": 3,
            "aqi": "yes",
            "alerts": "no"
        }
        return f"{BASE_WEATHER_API_URL}forecast.json?key={self.api_key}&{parse.urlencode(params)}"

    def build_history_url(self, city, date_str):
        params = {
            "q": city,
            "dt": date_str
        }
        return f"{BASE_WEATHER_API_URL}history.json?key={self.api_key}&{parse.urlencode(params)}"

    def run_forecast_etl(self):
        for city in self.cities:
            url = self.build_forecast_url(city)
            logging.info(f"Fetching forecast for {city}")
            data = self.fetch_data(url)
            if data:
                self.extract_and_write(data, mode="forecast")

    def run_history_etl(self):
        today = datetime.date.today()
        for i in range(1, 4):
            history_date = (today - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
            for city in self.cities:
                url = self.build_history_url(city, history_date)
                logging.info(f"Fetching history for {city} on {history_date}")
                data = self.fetch_data(url)
                if data:
                    self.extract_and_write(data, mode="history")

    def close_connection(self):
        self.cursor.close()
        self.conn.close()

    def start(self):
        try:
            self.run_forecast_etl()
            self.run_history_etl()
        finally:
            self.close_connection()


if __name__ == "__main__":
    WeatherExtractor().start()
