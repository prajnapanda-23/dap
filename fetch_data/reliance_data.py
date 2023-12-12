from datetime import datetime
from pymongo import MongoClient
import json
from datetime import datetime

# Connect to MongoDB
client = MongoClient("mongodb://%s:%s@127.0.0.1" % ("dap", "dap"))
db = client["Reliance_stock"]
collection_name = "DAP_PROJECT"

# Drop the existing collection to delete all data
#db[collection_name].drop()

# Load JSON data from file
with open("../dataset/reliance.json", "r") as file:
    data = json.load(file)

# Extract relevant data from the loaded JSON
meta_data = data.get('Meta Data', {})
symbol_key = meta_data.get('2. Symbol', 'Unknown')

time_series_data = data.get('Time Series (Daily)', {})
daily_data_list = []

for date, values in time_series_data.items():
    # Convert string values to appropriate types
    values['1. open'] = float(values.get('1. open', 0))
    values['2. high'] = float(values.get('2. high', 0))
    values['3. low'] = float(values.get('3. low', 0))
    values['4. close'] = float(values.get('4. close', 0))
    values['5. volume'] = int(values.get('5. volume', 0))

    # Convert date string to datetime object
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    values['date'] = date_obj
    values['symbol'] = symbol_key

    daily_data_list.append(values)

# Insert documents into MongoDB
if daily_data_list:
    db[collection_name].insert_many(daily_data_list)
    print("Data inserted successfully.")
else:
    print("No data to insert.")
