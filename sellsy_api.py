import hashlib
import hmac
import time
import json
import requests
import uuid
import os
from dotenv import load_dotenv

load_dotenv()

def sellsy_request(method, params):
    url = "https://api.sellsy.com/method"

    request = {
        'method': method,
        'params': params
    }

    headers = {
        'Content-Type': 'application/json'
    }

    oauth = {
        'oauth_consumer_key': os.getenv("SELLSY_API_CONSUMER_TOKEN"),
        'oauth_token': os.getenv("SELLSY_API_USER_TOKEN"),
        'oauth_nonce': str(uuid.uuid4().hex),
        'oauth_timestamp': str(int(time.time())),
        'oauth_signature_method': 'HMAC-SHA1',
        'oauth_version': '1.0'
    }

    base_str = "&".join([
        "POST", 
        requests.utils.quote(url, ""),
        requests.utils.quote("&".join(f"{k}={oauth[k]}" for k in sorted(oauth)), "")
    ])

    signing_key = f"{os.getenv('SELLSY_API_CONSUMER_SECRET')}&{os.getenv('SELLSY_API_USER_SECRET')}"
    signature = hmac.new(signing_key.encode(), base_str.encode(), hashlib.sha1).digest().hex()
    oauth['oauth_signature'] = signature

    auth_header = "OAuth " + ", ".join(f'{k}="{v}"' for k, v in oauth.items())

    response = requests.post(url, headers={**headers, 'Authorization': auth_header}, json=request)
    return response.json()
