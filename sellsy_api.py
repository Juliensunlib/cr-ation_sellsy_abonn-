import os
import time
import hashlib
import hmac
import base64
import json
import requests
from urllib.parse import urlencode

# Clés API Sellsy V1 depuis les secrets GitHub
CONSUMER_TOKEN = os.getenv("SELLSY_API_CONSUMER_TOKEN")
CONSUMER_SECRET = os.getenv("SELLSY_API_CONSUMER_SECRET")
USER_TOKEN = os.getenv("SELLSY_API_USER_TOKEN")
USER_SECRET = os.getenv("SELLSY_API_USER_SECRET")

SELLSY_API_URL = "https://apifeed.sellsy.com/0/"

def sellsy_request(method, params):
    request_data = {
        'method': method,
        'params': params
    }

    # Construction du header OAuth1 manuellement
    oauth_nonce = base64.b64encode(os.urandom(32)).decode('utf-8').rstrip('=')
    oauth_timestamp = str(int(time.time()))
    oauth_params = {
        'oauth_consumer_key': CONSUMER_TOKEN,
        'oauth_token': USER_TOKEN,
        'oauth_nonce': oauth_nonce,
        'oauth_timestamp': oauth_timestamp,
        'oauth_signature_method': 'HMAC-SHA1',
        'oauth_version': '1.0'
    }

    # Construction de la base string
    base_params = oauth_params.copy()
    base_string = "POST&" + urlencode({'': SELLSY_API_URL})[1:] + "&" + urlencode(sorted(base_params.items()))
    signing_key = f"{CONSUMER_SECRET}&{USER_SECRET}"
    signature = base64.b64encode(hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()).decode()
    oauth_params['oauth_signature'] = signature

    auth_header = 'OAuth ' + ', '.join([f'{k}="{v}"' for k, v in oauth_params.items()])
    headers = {
        'Authorization': auth_header,
        'Content-Type': 'application/json'
    }

    response = requests.post(SELLSY_API_URL, headers=headers, data=json.dumps(request_data))
    return response.json()
