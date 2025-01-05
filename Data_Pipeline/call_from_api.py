import requests
import yaml



# Load the YAML file
with open("../Config/Api_key", "r") as file:
    config = yaml.safe_load(file)

# Access the API key
api_key = config['AlphaAdvantage']['key']



# replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&interval=5min&apikey={api_key}'
r = requests.get(url)
data = r.json()

print(data)



