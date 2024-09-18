#!/usr/bin/env python
# Import the necessary libraries
import sqlite3
import subprocess
import json
import requests
import datetime

# Define the database file and the MQTT broker
db_file = '/var/lib/weewx/weewx.sdb'
mqtt_broker = 'YOUR_MQTT_BROKER'
mqtt_topic = 'YOUR_MQTT_TOPIC'
mqtt_username = 'USER'
mqtt_password = 'PASSWORD'

# Connect to the database
try:
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    print("Connected to the database successfully.")
except sqlite3.Error as e:
    print(f"Failed to connect to the database: {e}")
    exit(1)

# Query the database for the latest coordinates
try:
    cursor.execute("SELECT dateTime, latitude, longitude FROM archive ORDER BY dateTime DESC LIMIT 1")
    print("Query executed successfully.")
except sqlite3.Error as e:
    print(f"Failed to execute query: {e}")
    conn.close()
    exit(1)

# Fetch the latest row from the query
row = cursor.fetchone()
print(f"Fetched {row} from the database.")

# Close the database connection
conn.close()
print("Database connection closed.")

# Get the altitude using the primary elevation API
if row:
    date_time, latitude, longitude = row
    altitude = None
    try:
        # Primary API: Open Elevation
        url = f"https://api.open-elevation.com/api/v1/lookup?locations={latitude},{longitude}"
        response = requests.get(url)
        data = response.json()
        altitude = data["results"][0]["elevation"]
        print(f"Estimated altitude: {altitude:.2f} meters")
    except requests.RequestException as e:
        print(f"Failed to retrieve altitude from primary API: {e}")
        try:
            # Backup API: Open-Meteo
            url = f"https://api.open-meteo.com/v1/elevation?latitude={latitude}&longitude={longitude}"
            response = requests.get(url)
            data = response.json()
            altitude = data["results"]["elevation"]
            print(f"Estimated altitude: {altitude:.2f} meters")
        except requests.RequestException as e:
            print(f"Failed to retrieve altitude from backup API: {e}")
            altitude = 0  # Assign a default value to altitude when both API calls fail

    # Publish the altitude to the MQTT topic using mosquitto_pub
    try:
        # Construct the payload
        payload = f"{{\"dateTime\": \"{date_time}\", \"altitude\": \"{altitude:.2f}\"}}"
        # Construct the mosquitto_pub command
        command = f"mosquitto_pub -u {mqtt_username} -P {mqtt_password} -h {mqtt_broker} -t {mqtt_topic} -m '{payload}'"
        # Execute the command with a timeout of 10 seconds
        subprocess.run(command, shell=True, check=True, timeout=30)
        print(f"Published altitude to topic {mqtt_topic}.")
        print(f"{payload}")
    except subprocess.TimeoutExpired:
        print("Timeout expired while publishing altitude.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to publish altitude: {e}")
else:
    print("No coordinates found in the database.")
