# 🌦️ Weather ETL Pipeline

Developed a robust ETL pipeline in Python to fetch, process, and store real-time and historical weather data using WeatherAPI. Integrated exponential backoff retry logic for reliable API consumption, structured logging for traceability, and upsert logic using MySQL to prevent duplicate entries. Externalized configuration (API key, cities, DB credentials) via secrets.ini for portability and secure deployments.

---

## 📌 Features

🔁 **ETL Pipeline**: Extracts weather data (forecast & historical) via WeatherAPI, transforms it, and loads it into a MySQL table using an OOP-based ETL pipeline.

🧠 **Smart Deduplication**: Prevents duplicate inserts using MySQL’s ON DUPLICATE KEY UPDATE based on a unique key (date, location, type).

🔄 **Retry Mechanism**: Automatically retries failed API calls up to 3 times with exponential backoff for robust network resilience.

🧪 **Robust Error Logging**: Logs warnings, errors, and full stack traces for better debugging and observability using Python’s logging and traceback modules.

🔐 **Secure & Configurable**: Reads city list, API key, and MySQL credentials from a secrets.ini file for security and flexibility.

🗂️ **Modular Design**: Separates concerns like URL building, database insertion, API fetching, and exception handling, making the code easy to maintain and extend.
