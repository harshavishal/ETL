import glob
import pandas as pd
from datetime import datetime
import requests
import json
import csv

#stores data from url to device
url1 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_1.json'
url2 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/bank_market_cap_2.json'
url3 = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Final%20Assignment/exchange_rates.csv'

response1 = requests.get(url1)
response2 = requests.get(url2)
response3 = requests.get(url3)

data1 = response1.json()
data2 = response2.json()
data3 = response3.text

with open('bank_market_cap_1.json', 'w') as file:
    json.dump(data1, file)

with open('bank_market_cap_2.json', 'w') as file:
    json.dump(data2, file)

with open('exchange_rates.csv', 'w', newline='') as file:
    file.write(data3)

def extract_from_json(file_to_process):
    print("Extract phase Started")
    dataframe = pd.read_json(file_to_process)
    return dataframe
def extract():
    file_to_process = 'bank_market_cap_1.json'
    data = extract_from_json(file_to_process)
    columns = ['Name', 'Market Cap (US$ Billion)']
    data = data[columns]  # Selecting specific columns from the DataFrame
    return data
extracted_data = extract()
print(extracted_data)
print("Extract phase Ended")
# Load the CSV file as a dataframe
dataframe = pd.read_csv('exchange_rates.csv', index_col=0)
# Find the exchange rate for British pounds (GBP)
exchange_rate = dataframe.loc['GBP']
# Print the exchange rate
print(exchange_rate)

#TRANSFORM
def transform(dataframe, exchange_rate):
    # Convert the 'Market Cap (US$ Billion)' column from USD to GBP
    dataframe['Market Cap (US$ Billion)'] = dataframe['Market Cap (US$ Billion)'] * exchange_rate
    # Round the 'Market Cap (US$ Billion)' column to 3 decimal places
    dataframe['Market Cap (US$ Billion)'] = dataframe['Market Cap (US$ Billion)'].round(3)
    # Rename the 'Market Cap (US$ Billion)' column to 'Market Cap (GBP$ Billion)'
    dataframe = dataframe.rename(columns={'Market Cap (US$ Billion)': 'Market Cap (GBP$ Billion)'})
    return dataframe
# Load the CSV file as a dataframe
dataframe = pd.read_csv('exchange_rates.csv', index_col=0)
# Find the exchange rate for British pounds (GBP)
exchange_rate = dataframe.loc['GBP','Rates']
# Print the exchange rate
print(exchange_rate)
# Call the transform function
print("Transform phase Started")
transformed_data = transform(extracted_data, exchange_rate)
# Print the transformed data
print(transformed_data)
print("Transform phase Ended")
#LOAD
def load_to_csv(dataframe):
    # Specify the output file name
    output_file = 'bank_market_cap_gbp.csv'

    # Save the DataFrame to a CSV file
    dataframe.to_csv(output_file, index=False)
print("Load phase Started")
# Call the load_to_csv function with the transformed data
load_to_csv(transformed_data)
print("Load phase Ended")
