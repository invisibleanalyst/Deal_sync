import requests
import os
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("ZOHO_CLIENT_ID")
client_secret = os.getenv("ZOHO_CLIENT_SECRET")
redirect_uri = "http://localhost"
authorization_code = "1000.26e693bfe4c4b226ce4781779789c21f.90b708b0fb9893d7744cc1e339c32db0"

url = "https://accounts.zoho.com/oauth/v2/token"

params = {
    "grant_type": "authorization_code",
    "client_id": client_id,
    "client_secret": client_secret,
    "redirect_uri": redirect_uri,
    "code": authorization_code
}

response = requests.post(url, params=params)
print(response.json())
