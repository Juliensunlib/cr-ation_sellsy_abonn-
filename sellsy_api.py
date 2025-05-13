import os
import json
import requests
from typing import Dict, Optional
from requests_oauthlib import OAuth1

class SellsyAPI:
    """API client pour Sellsy v2."""
    # URL de base pour l'API v2
    API_ENDPOINT = "https://api.sellsy.com/v2"
    
    def __init__(self, consumer_token, consumer_secret, user_token, user_secret, logger):
        self.consumer_token = consumer_token
        self.consumer_secret = consumer_secret
        self.user_token = user_token
        self.user_secret = user_secret
        self.logger = logger
        
        # Création de l'objet OAuth1 pour l'authentification
        self.oauth = OAuth1(
            self.consumer_token,
            self.consumer_secret,
            self.user_token,
            self.user_secret
        )
    
    def test_authentication(self) -> bool:
        """
        Teste l'authentification à l'API Sellsy.
        
        Returns:
            bool: True si l'authentification est réussie, False sinon
        """
        try:
            # Tester l'authenticité avec l'endpoint 'myself' qui retourne les infos du compte
            response = requests.get(
                f"{self.API_ENDPOINT}/myself",
                auth=self.oauth
            )
            
            # Log pour débogage
            self.logger.debug(f"Test d'authentification - Status code: {response.status_code}")
            
            if response.status_code == 200:
                self.logger.info("✅ Authentification Sellsy réussie")
                return True
            else:
                self.logger.error(f"❌ Échec de l'authentification Sellsy: {response.status_code}")
                self.logger.error(f"Détails: {response.text}")
                return False
                
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur lors du test d'authentification: {e}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"Status code: {e.response.status_code}")
                self.logger.error(f"Détails: {e.response.text}")
            return False
    
    def create_client(self, client_data: Dict) -> Optional[Dict]:
        """
        Crée un nouveau client dans Sellsy.
        
        Args:
            client_data: Données du client à créer
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        self.logger.info("📤 Tentative de création d'un client dans Sellsy")
        
        # Vérifier l'authentification avant de procéder
        if not self.test_authentication():
            return None
        
        try:
            # Création du client avec l'API v2
            response = requests.post(
                f"{self.API_ENDPOINT}/contacts",
                json=client_data,
                auth=self.oauth
            )
            
            # Log des détails pour débogage
            self.logger.debug(f"Status code: {response.status_code}")
            self.logger.debug(f"URL: {self.API_ENDPOINT}/contacts")
            self.logger.debug(f"Données envoyées: {json.dumps(client_data)}")
            
            # Vérification de la réponse
            response.raise_for_status()
            
            result = response.json()
            self.logger.info(f"✅ Client créé avec succès dans Sellsy")
            return {"status": "success", "response": result}
            
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur lors de la création du client: {e}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"Status code: {e.response.status_code}")
                self.logger.error(f"Détails: {e.response.text}")
            return None
    
    def get_client(self, client_id: str) -> Optional[Dict]:
        """
        Récupère les détails d'un client.
        
        Args:
            client_id: ID du client à récupérer
            
        Returns:
            Données du client ou None en cas d'erreur
        """
        if not self.test_authentication():
            return None
            
        try:
            response = requests.get(
                f"{self.API_ENDPOINT}/contacts/{client_id}",
                auth=self.oauth
            )
            
            response.raise_for_status()
            return {"status": "success", "response": response.json()}
            
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur lors de la récupération du client {client_id}: {e}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"Status code: {e.response.status_code}")
                self.logger.error(f"Détails: {e.response.text}")
            return None
    
    def search_clients(self, search_term: str = None, limit: int = 100) -> Optional[Dict]:
        """
        Recherche des clients dans Sellsy.
        
        Args:
            search_term: Terme de recherche
            limit: Nombre maximum de résultats
            
        Returns:
            Liste des clients ou None en cas d'erreur
        """
        if not self.test_authentication():
            return None
            
        try:
            params = {"limit": limit}
            if search_term:
                params["search"] = search_term
                
            response = requests.get(
                f"{self.API_ENDPOINT}/contacts",
                params=params,
                auth=self.oauth
            )
            
            response.raise_for_status()
            return {"status": "success", "response": response.json()}
            
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur lors de la recherche de clients: {e}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"Status code: {e.response.status_code}")
                self.logger.error(f"Détails: {e.response.text}")
            return None
