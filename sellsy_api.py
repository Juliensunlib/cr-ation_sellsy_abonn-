import os
import time
import json
import requests
from requests_oauthlib import OAuth1

class SellsyAPI:
    BASE_URL = "https://api.sellsy.com/0/"
    
    def __init__(self, consumer_token, consumer_secret, user_token, user_secret):
        self.oauth = OAuth1(
            consumer_token,
            consumer_secret,
            user_token,
            user_secret
        )
    
    def make_request(self, method, params):
        """
        Effectue une requête à l'API Sellsy en utilisant OAuth1
        """
        try:
            response = requests.post(
                self.BASE_URL,
                data={"request": 1, "io_mode": "json"},
                files={"do_in": (None, json.dumps({"method": method, "params": params}))},
                auth=self.oauth
            )
            
            response.raise_for_status()
            return response.json()
        
        except requests.RequestException as e:
            print(f"Erreur lors de la requête Sellsy {method}: {e}")
            return None

    def create_client(self, client_data):
        """
        Crée un client dans Sellsy
        
        client_data doit contenir au minimum:
        - name: nom du client
        - email: email du client
        """
        return self.make_request("Client.create", {"third": client_data})

    def search_client(self, query):
        """
        Recherche un client dans Sellsy
        """
        return self.make_request("Client.getList", {"search": {"contains": query}})

    def get_client(self, client_id):
        """
        Récupère les détails d'un client
        """
        return self.make_request("Client.getOne", {"clientid": client_id})
