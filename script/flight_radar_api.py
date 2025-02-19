import requests

url = "https://api.aviationstack.com/v1/flights?access_key=d0c318dffed1d4677589349b81334616"

response = requests.get(url)

print(response.json())