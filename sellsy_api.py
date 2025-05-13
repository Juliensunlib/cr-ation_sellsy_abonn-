import os
import json
import time
import requests
import logging
from typing import Dict, List, Optional
from datetime import datetime, timedelta

class SellsyAPI:
    """
    Client API pour Sellsy v2 basé sur l'authentification OAuth 2.0.
    """
    
    # URL de l'API v2
    API_BASE_URL = "https://api.sellsy.com/v2"
    # URL correcte pour l'authentification OAuth2 de Sellsy v2
    AUTH_URL = "https://api.sellsy.com/oauth2/access-token"
    
    def __init__(self, client_id, client_secret, access_token=None, refresh_token=None, logger=None):
        """
        Initialise le client API Sellsy v2.
        
        Args:
            client_id: Identifiant client OAuth 2.0
            client_secret: Secret client OAuth 2.0
            access_token: Token d'accès (optionnel)
            refresh_token: Token de rafraîchissement (optionnel)
            logger: Logger pour journaliser les actions
        """
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_expires_at = None
        
        # Définir un logger par défaut si aucun n'est fourni
        if logger is None:
            self.logger = logging.getLogger('SellsyAPI')
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
        
        self.logger.debug("SellsyAPI v2 initialisée avec succès")
    
    def _get_auth_header(self) -> Dict:
        """
        Obtient l'en-tête d'authentification pour les requêtes API.
        
        Returns:
            En-tête d'authentification
        """
        if self._is_token_expired():
            self.refresh_access_token()
            
        return {"Authorization": f"Bearer {self.access_token}"}
    
    def _is_token_expired(self) -> bool:
        """
        Vérifie si le token d'accès est expiré.
        
        Returns:
            True si le token est expiré ou non défini, False sinon
        """
        if not self.access_token or not self.token_expires_at:
            return True
            
        return datetime.now() >= self.token_expires_at
    
    def get_access_token(self) -> bool:
        """
        Obtient un nouveau token d'accès en utilisant le flux d'authentification client credentials.
        
        Returns:
            True si l'obtention du token a réussi, False sinon
        """
        try:
            self.logger.info("🔄 Obtention d'un nouveau token d'accès...")
            
            # En-têtes pour l'authentification Basic
            auth = (self.client_id, self.client_secret)
            
            # Paramètres de la requête
            payload = {
                "grant_type": "client_credentials"
            }
            
            # En-têtes pour spécifier le type de contenu
            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }
            
            response = requests.post(
                self.AUTH_URL, 
                auth=auth,
                data=payload,
                headers=headers
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data["access_token"]
                # Le refresh token est généralement fourni avec le grant_type=authorization_code
                if "refresh_token" in data:
                    self.refresh_token = data["refresh_token"]
                # Calcul de la date d'expiration (généralement 3600 secondes)
                expires_in = data.get("expires_in", 3600)
                self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)  # -60 pour marge de sécurité
                
                self.logger.info(f"✅ Token d'accès obtenu avec succès (expire dans {expires_in} secondes)")
                return True
            else:
                self.logger.error(f"❌ Échec d'obtention du token: {response.status_code}")
                self.logger.error(f"Détails: {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'obtention du token: {str(e)}")
            return False
    
    def refresh_access_token(self) -> bool:
        """
        Rafraîchit le token d'accès en utilisant le token de rafraîchissement.
        
        Returns:
            True si le rafraîchissement a réussi, False sinon
        """
        if self.refresh_token:
            try:
                self.logger.info("🔄 Rafraîchissement du token d'accès...")
                
                auth = (self.client_id, self.client_secret)
                
                payload = {
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token
                }
                
                headers = {
                    "Content-Type": "application/x-www-form-urlencoded"
                }
                
                response = requests.post(
                    self.AUTH_URL, 
                    auth=auth,
                    data=payload,
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data["access_token"]
                    if "refresh_token" in data:
                        self.refresh_token = data["refresh_token"]
                    expires_in = data.get("expires_in", 3600)
                    self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
                    
                    self.logger.info(f"✅ Token d'accès rafraîchi avec succès")
                    return True
                else:
                    self.logger.error(f"❌ Échec du rafraîchissement du token: {response.status_code}")
                    self.logger.error(f"Détails: {response.text}")
                    return False
                    
            except Exception as e:
                self.logger.error(f"❌ Erreur lors du rafraîchissement du token: {str(e)}")
                return False
        else:
            # Si pas de refresh_token, on utilise le flux client credentials
            return self.get_access_token()
    
    def request_api(self, method: str, endpoint: str, data: Dict = None, params: Dict = None) -> Optional[Dict]:
        """
        Effectue une requête à l'API Sellsy v2.
        
        Args:
            method: Méthode HTTP (GET, POST, PUT, DELETE)
            endpoint: Point de terminaison API (sans le préfixe de base)
            data: Données à envoyer (pour POST/PUT)
            params: Paramètres de requête (pour GET)
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        try:
            # S'assurer que nous avons un token valide
            if self._is_token_expired():
                if not self.get_access_token():
                    self.logger.error("❌ Impossible d'obtenir un token d'accès valide")
                    return None
            
            # Préparation de l'URL
            url = f"{self.API_BASE_URL}/{endpoint.lstrip('/')}"
            
            # Préparation des en-têtes
            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            self.logger.debug(f"Requête API v2: {method} {url}")
            if data:
                self.logger.debug(f"Données: {json.dumps(data)[:200]}...")
            if params:
                self.logger.debug(f"Paramètres: {params}")
            
            # Exécution de la requête
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data if data else None,
                params=params if params else None
            )
            
            # Vérification du statut de la réponse
            if response.status_code in [200, 201, 202, 204]:
                try:
                    if response.content:
                        result = response.json()
                        self.logger.debug(f"Réponse reçue: {json.dumps(result)[:200]}...")
                        return result
                    return {"status": "success"}
                except json.JSONDecodeError:
                    self.logger.error(f"❌ Réponse non-JSON: {response.text[:200]}")
                    return None
            elif response.status_code == 401:
                # Token expiré ou invalide, on tente de rafraîchir
                self.logger.warning("⚠️ Token d'accès expiré. Tentative de rafraîchissement...")
                if self.refresh_access_token():
                    # On réessaie la requête avec le nouveau token
                    return self.request_api(method, endpoint, data, params)
                return None
            else:
                self.logger.error(f"❌ Erreur HTTP: {response.status_code}")
                self.logger.error(f"Détails: {response.text}")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur de connexion: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Erreur inattendue: {str(e)}")
            self.logger.exception("Détails:")
            return None
    
    def test_authentication(self) -> bool:
        """
        Teste l'authentification en récupérant les informations sur l'API.
        
        Returns:
            True si l'authentification est réussie, False sinon
        """
        self.logger.info("🔄 Test d'authentification Sellsy v2...")
        
        try:
            # Récupération du compte utilisateur pour tester l'authentification
            response = self.request_api("GET", "/myself")
            
            if response:
                self.logger.info("✅ Authentification réussie!")
                return True
            else:
                self.logger.error("❌ Échec d'authentification")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors du test d'authentification: {str(e)}")
            return False
    
    def create_client(self, client_data: Dict) -> Optional[Dict]:
        """
        Crée un nouveau client dans Sellsy.
        
        Args:
            client_data: Données du client à créer
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        self.logger.info("🔄 Création d'un nouveau client dans Sellsy v2")
        
        try:
            # Conversion du format des données pour l'API v2
            v2_client_data = self._prepare_client_data_for_v2(client_data)
            
            # API v2 utilise différents endpoints pour individus et entreprises
            is_individual = client_data.get("third", {}).get("type") == "person"
            
            if is_individual:
                endpoint = "/individuals"
            else:
                endpoint = "/companies"
            
            # Exécution de la requête
            self.logger.debug(f"Données client formatées pour v2: {v2_client_data}")
            response = self.request_api("POST", endpoint, v2_client_data)
            
            if response:
                # Extraction de l'ID client de la réponse
                client_id = response.get("id")
                self.logger.info(f"✅ Client créé avec succès! ID: {client_id}")
                return {"status": "success", "response": client_id}
            else:
                self.logger.error("❌ Échec de création du client")
                return {"status": "error", "error": "Échec de création du client"}
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la création du client: {str(e)}")
            self.logger.exception("Détails:")
            return {"status": "error", "error": str(e)}
    
    def _prepare_client_data_for_v2(self, old_data: Dict) -> Dict:
        """
        Convertit les données client du format v1 au format v2 attendu par l'API.
        
        Args:
            old_data: Données client au format v1
            
        Returns:
            Données client au format v2
        """
        # Extraction des données de l'ancien format
        third = old_data.get("third", {})
        contact = old_data.get("contact", {})
        address = old_data.get("address", {})
        
        # Déterminer si c'est un particulier ou une entreprise
        is_individual = third.get("type") == "person"
        
        if is_individual:
            # Format pour les particuliers (individuals)
            result = {
                "first_name": contact.get("firstname", ""),
                "last_name": contact.get("name", ""),
                "email": contact.get("email", ""),
                "phone_number": contact.get("tel", ""),
                "civil": {
                    "civil": "mr" if contact.get("civility") == "man" else "mrs"
                },
                "addresses": []
            }
        else:
            # Format pour les entreprises (companies)
            result = {
                "name": third.get("name", ""),
                "email": third.get("email", ""),
                "phone_number": third.get("tel", ""),
                "note": third.get("notes", ""),
                "addresses": []
            }
        
        # Ajout de l'adresse si présente
        if address:
            new_address = {
                "name": address.get("name", "Adresse principale"),
                "address": address.get("part1", ""),
                "zip_code": address.get("zip", ""),
                "city": address.get("town", ""),
                "country_code": address.get("countrycode", "FR")
            }
            result["addresses"].append(new_address)
        
        # Ajout du contact pour les entreprises
        if not is_individual and contact:
            result["contacts"] = [{
                "first_name": contact.get("firstname", ""),
                "last_name": contact.get("name", ""),
                "email": contact.get("email", ""),
                "phone_number": contact.get("tel", "")
            }]
        
        return result
    
    def get_client(self, client_id: str, is_individual: bool = False) -> Optional[Dict]:
        """
        Récupère les informations d'un client par son ID.
        
        Args:
            client_id: ID du client à récupérer
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            Informations du client ou None en cas d'erreur
        """
        endpoint = f"/individuals/{client_id}" if is_individual else f"/companies/{client_id}"
        self.logger.info(f"🔄 Récupération du client ID: {client_id}")
        
        return self.request_api("GET", endpoint)
    
    def search_clients(self, search_term=None, limit=100) -> Optional[Dict]:
        """
        Recherche des clients dans Sellsy.
        
        Args:
            search_term: Terme de recherche (facultatif)
            limit: Nombre maximum de résultats à retourner
            
        Returns:
            Liste des clients ou None en cas d'erreur
        """
        self.logger.info(f"🔄 Recherche de clients" + (f" avec terme: {search_term}" if search_term else ""))
        
        # Paramètres de recherche
        params = {
            "limit": limit,
            "offset": 0
        }
        
        if search_term:
            params["search"] = search_term
        
        # Recherche dans les entreprises et particuliers
        companies = self.request_api("GET", "/companies", params=params) or {"data": []}
        individuals = self.request_api("GET", "/individuals", params=params) or {"data": []}
        
        # Combiner les résultats
        results = []
        results.extend(companies.get("data", []))
        results.extend(individuals.get("data", []))
        
        return {"data": results[:limit]}
