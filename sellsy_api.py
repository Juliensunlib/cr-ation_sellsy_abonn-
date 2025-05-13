import os
import json
import time
import random
import hashlib
import requests
from typing import Dict, Optional
import urllib.parse

class SellsyAPI:
    """API client pour Sellsy v1."""
    # URL de base pour l'API v1
    API_ENDPOINT = "https://apifeed.sellsy.com/0"
    
    def __init__(self, consumer_token, consumer_secret, user_token, user_secret, logger):
        self.consumer_token = consumer_token
        self.consumer_secret = consumer_secret
        self.user_token = user_token
        self.user_secret = user_secret
        self.logger = logger
    
    def _generate_oauth_params(self):
        """
        Génère les paramètres OAuth pour l'API Sellsy v1.
        
        Returns:
            dict: Les paramètres OAuth
        """
        nonce = str(random.getrandbits(64))
        timestamp = str(int(time.time()))
        
        # Construction correcte de la signature pour Sellsy API v1
        # La signature de base doit être les secrets séparés par & SANS URL ENCODE
        signature = f"{self.consumer_secret}&{self.user_secret}"
        
        oauth_params = {
            'oauth_consumer_key': self.consumer_token,
            'oauth_token': self.user_token,
            'oauth_nonce': nonce,
            'oauth_timestamp': timestamp,
            'oauth_signature_method': 'PLAINTEXT',
            'oauth_version': '1.0',
            'oauth_signature': signature
        }
        
        return oauth_params
    
    def _make_request(self, method: Dict) -> Optional[Dict]:
        """
        Effectue une requête à l'API Sellsy v1.
        
        Args:
            method: Méthode API et paramètres associés
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        try:
            # Génération des paramètres OAuth
            oauth_params = self._generate_oauth_params()
            
            # Construction du corps de la requête
            request_data = {
                'request': 1,
                'io_mode': 'json',
                'do_in': json.dumps(method)
            }
            
            # Cloner les oauth_params pour le log sans révéler les secrets
            safe_oauth = oauth_params.copy()
            if 'oauth_signature' in safe_oauth:
                safe_oauth['oauth_signature'] = "***SIGNATURE-HIDDEN***"
            self.logger.debug(f"OAuth params: {safe_oauth}")
            self.logger.debug(f"Request data: {request_data}")
            
            # CORRECTION: Fusionner les paramètres OAuth et les données de requête dans le corps
            # Pour Sellsy API v1, tous les paramètres doivent être dans le corps de la requête
            data = {**oauth_params, **request_data}
            
            # Envoi de la requête POST avec tous les paramètres dans le corps
            response = requests.post(
                self.API_ENDPOINT,
                data=data  # Tous les paramètres sont dans le corps
            )
            
            # Enregistrement de la réponse brute pour débogage
            self.logger.debug(f"Status code: {response.status_code}")
            self.logger.debug(f"Response headers: {dict(response.headers)}")
            self.logger.debug(f"Response content: {response.text[:500]}")
            
            # Vérification de la réponse
            if response.status_code != 200:
                self.logger.error(f"❌ Statut HTTP erreur: {response.status_code}")
                # Vérifier si c'est une erreur OAuth et l'afficher
                if "oauth_problem" in response.text:
                    self.logger.error(f"❌ Erreur OAuth: {response.text}")
                return None
            
            # Tentative de conversion de la réponse en JSON
            try:
                result = response.json()
            except ValueError:
                self.logger.error(f"❌ Réponse non-JSON: {response.text}")
                return None
            
            # Vérification des erreurs dans la réponse
            if isinstance(result, dict) and "error" in result:
                self.logger.error(f"❌ Erreur API Sellsy: {result['error']}")
                return None
            
            return result
            
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur lors de la requête à l'API Sellsy: {str(e)}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"Status code: {e.response.status_code}")
                self.logger.error(f"Détails: {e.response.text if hasattr(e.response, 'text') else 'Pas de réponse'}")
            return None
        except ValueError as e:
            # Erreur lors du décodage JSON
            self.logger.error(f"❌ Erreur de décodage JSON: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Erreur inattendue: {str(e)}")
            return None
    
    def test_authentication(self) -> bool:
        """
        Teste l'authentification à l'API Sellsy v1.
        
        Returns:
            bool: True si l'authentification est réussie, False sinon
        """
        self.logger.info("🔄 Test d'authentification Sellsy...")
        
        try:
            # Utilisation de la méthode Infos.getInfos pour tester l'authentification
            method = {
                "method": "Infos.getInfos"
            }
            
            result = self._make_request(method)
            
            if result:
                self.logger.info("✅ Authentification Sellsy réussie")
                return True
            else:
                self.logger.error("❌ Échec de l'authentification Sellsy")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors du test d'authentification: {str(e)}")
            return False
    
    def create_client(self, client_data: Dict) -> Optional[Dict]:
        """
        Crée un nouveau client dans Sellsy v1.
        
        Args:
            client_data: Données du client à créer formatées pour l'API v2
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        self.logger.info("📤 Tentative de création d'un client dans Sellsy")
        
        # Vérifier l'authentification avant de procéder
        if not self.test_authentication():
            return None
        
        try:
            # Conversion des données du format v2 au format v1
            v1_client_data = self._convert_v2_to_v1_format(client_data)
            
            # Création de la requête pour l'API v1
            method = {
                "method": "Client.create",
                "params": v1_client_data
            }
            
            # Masquer les données sensibles pour le log
            log_data = method.copy()
            if "params" in log_data and "third" in log_data["params"] and "email" in log_data["params"]["third"]:
                log_data["params"]["third"]["email"] = "***@***.com"
            if "params" in log_data and "contact" in log_data["params"] and "email" in log_data["params"]["contact"]:
                log_data["params"]["contact"]["email"] = "***@***.com"
                
            self.logger.debug(f"Données envoyées à l'API v1: {json.dumps(log_data)}")
            
            # Envoi de la requête
            result = self._make_request(method)
            
            if result:
                client_id = result.get("response")
                self.logger.info(f"✅ Client créé avec succès dans Sellsy (ID: {client_id})")
                return {"status": "success", "response": {"id": client_id}}
            else:
                self.logger.error("❌ Échec de la création du client")
                return None
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la création du client: {str(e)}")
            return None
    
    def _convert_v2_to_v1_format(self, v2_data: Dict) -> Dict:
        """
        Convertit les données du format API v2 au format API v1.
        
        Args:
            v2_data: Données client au format API v2
            
        Returns:
            Données client au format API v1
        """
        # Récupération des données de base
        name = v2_data.get("name", "")
        email = v2_data.get("email", "")
        phone = v2_data.get("phone", "")
        
        # Récupération des données de contact
        contact = v2_data.get("contact", {})
        lastname = contact.get("name", "")
        firstname = contact.get("firstName", "")
        
        # Récupération des données d'adresse
        address = v2_data.get("address", {})
        street = address.get("address", "")
        zipcode = address.get("zipcode", "")
        town = address.get("city", "")
        country_code = address.get("countryCode", "FR")
        
        # Construction des données au format v1
        v1_data = {
            "third": {
                "name": name,
                "email": email,
                "tel": phone,
                "type": "person" if v2_data.get("type") == "person" else "corporation"
            },
            "contact": {
                "name": lastname,
                "firstname": firstname,
                "email": email,
                "tel": phone,
                "position": "Client",
                "civil": "man"  # Valeur par défaut
            },
            "address": {
                "name": "Adresse principale",
                "part1": street,
                "zip": zipcode,
                "town": town,
                "countrycode": country_code
            }
        }
        
        return v1_data
    
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
            # Création de la requête pour l'API v1
            method = {
                "method": "Client.getOne",
                "params": {
                    "clientid": client_id
                }
            }
            
            # Envoi de la requête
            result = self._make_request(method)
            
            if result:
                return {"status": "success", "response": result.get("response")}
            else:
                self.logger.error(f"❌ Échec de la récupération du client {client_id}")
                return None
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération du client {client_id}: {str(e)}")
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
            # Création de la requête pour l'API v1
            method = {
                "method": "Client.getList",
                "params": {
                    "pagination": {
                        "nbperpage": limit,
                        "pagenum": 1
                    }
                }
            }
            
            # Ajout du terme de recherche si présent
            if search_term:
                method["params"]["search"] = {
                    "contains": search_term
                }
            
            # Envoi de la requête
            result = self._make_request(method)
            
            if result:
                return {"status": "success", "response": result.get("response")}
            else:
                self.logger.error("❌ Échec de la recherche de clients")
                return None
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la recherche de clients: {str(e)}")
            return None
