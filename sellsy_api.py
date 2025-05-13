import os
import json
import time
import random
import hashlib
import requests
import urllib.parse
import logging
from typing import Dict, List, Optional

class SellsyAPI:
    """
    Client API pour Sellsy v1 basé sur l'authentification OAuth conforme à la documentation Sellsy.
    """
    
    # URL de l'API selon la documentation
    API_URL = "https://apifeed.sellsy.com/0"
    REQ_TOKEN_URL = "https://apifeed.sellsy.com/0/request_token"
    ACC_TOKEN_URL = "https://apifeed.sellsy.com/0/access_token"
    
    def __init__(self, consumer_token, consumer_secret, user_token, user_secret, logger=None):
        """
        Initialise le client API Sellsy.
        
        Args:
            consumer_token: Token de l'application
            consumer_secret: Secret de l'application
            user_token: Token utilisateur (pour application privée)
            user_secret: Secret utilisateur (pour application privée)
            logger: Logger pour journaliser les actions
        """
        self.consumer_token = consumer_token
        self.consumer_secret = consumer_secret
        self.user_token = user_token
        self.user_secret = user_secret
        
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
        
        self.logger.debug("SellsyAPI initialisée avec succès")
    
    def request_api(self, request_settings: Dict) -> Optional[Dict]:
        """
        Effectue une requête à l'API Sellsy en suivant la documentation officielle.
        
        Args:
            request_settings: Paramètres de la requête (méthode et paramètres)
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        try:
            # Vérification que les tokens sont présents
            if not all([self.consumer_token, self.consumer_secret, self.user_token, self.user_secret]):
                self.logger.error("❌ Authentification incomplète: tokens/secrets manquants")
                return None
            
            # Préparation des paramètres OAuth
            oauth_params = self._prepare_oauth_params()
            
            # Préparation du corps de la requête
            request_data = {
                'request': 1,
                'io_mode': 'json',
                'do_in': json.dumps(request_settings)
            }
            
            # Fusion des paramètres OAuth et des données de requête
            data = {**oauth_params, **request_data}
            
            # Log des données sensibles masquées
            debug_data = data.copy()
            if 'oauth_signature' in debug_data:
                debug_data['oauth_signature'] = '******'
            if 'oauth_consumer_key' in debug_data:
                debug_data['oauth_consumer_key'] = '***'
            if 'oauth_token' in debug_data:
                debug_data['oauth_token'] = '***'
            
            self.logger.debug(f"Requête API: URL={self.API_URL}")
            self.logger.debug(f"Données de requête: {debug_data}")
            
            # Envoi de la requête POST
            response = requests.post(
                self.API_URL,
                data=data
            )
            
            # Vérification du statut de la réponse
            if response.status_code != 200:
                self.logger.error(f"❌ Erreur HTTP: {response.status_code}")
                self.logger.error(f"Détails: {response.text}")
                
                # Vérifier s'il s'agit d'une erreur OAuth spécifique
                if "oauth_problem" in response.text:
                    self.logger.error(f"❌ Erreur OAuth: {response.text}")
                return None
            
            # Traitement de la réponse
            try:
                result = response.json()
                
                # Vérification des erreurs dans la réponse JSON
                if isinstance(result, dict):
                    if result.get('status') == 'error':
                        self.logger.error(f"❌ Erreur API: {result.get('error')}")
                        return result
                
                self.logger.debug(f"Réponse reçue: {json.dumps(result)[:200]}...")
                return result
            except json.JSONDecodeError:
                self.logger.error(f"❌ Réponse non-JSON: {response.text[:200]}")
                return None
                
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur de connexion: {str(e)}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Erreur inattendue: {str(e)}")
            self.logger.exception("Détails:")
            return None
    
    def _prepare_oauth_params(self) -> Dict:
        """
        Prépare les paramètres OAuth conformément à la documentation Sellsy.
        
        Returns:
            Paramètres OAuth
        """
        # Génération des valeurs OAuth requises
        nonce = str(random.getrandbits(64))
        timestamp = str(int(time.time()))
        
        # Création de la signature
        # Correction: Pour Sellsy, la signature est simplement concaténée avec &, pas d'encodage URL ici
        signature = f"{self.consumer_secret}&{self.user_secret}"
        
        # Paramètres OAuth
        oauth_params = {
            'oauth_consumer_key': self.consumer_token,
            'oauth_token': self.user_token,
            'oauth_signature_method': 'PLAINTEXT',
            'oauth_signature': signature,
            'oauth_timestamp': timestamp,
            'oauth_nonce': nonce,
            'oauth_version': '1.0'
        }
        
        return oauth_params
    
    def create_client(self, client_data: Dict) -> Optional[Dict]:
        """
        Crée un nouveau client dans Sellsy.
        
        Args:
            client_data: Données du client à créer (format v2)
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        self.logger.info("🔄 Création d'un nouveau client dans Sellsy")
        
        try:
            # Conversion des données client au format v1
            v1_client_data = self._convert_v2_to_v1_format(client_data)
            
            # Préparation de la requête
            request_settings = {
                "method": "Client.create",
                "params": v1_client_data
            }
            
            # Exécution de la requête
            self.logger.debug(f"Données client formatées: {v1_client_data}")
            response = self.request_api(request_settings)
            
            if response and response.get('status') == 'success':
                # Extraction de l'ID client de la réponse
                client_id = response.get('response')
                self.logger.info(f"✅ Client créé avec succès! ID: {client_id}")
                return {"status": "success", "response": client_id}
            else:
                error_msg = "Réponse API invalide"
                if response and 'error' in response:
                    error_msg = response.get('error')
                self.logger.error(f"❌ Échec de création du client: {error_msg}")
                return response
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la création du client: {str(e)}")
            self.logger.exception("Détails:")
            return None
    
    def _convert_v2_to_v1_format(self, v2_data: Dict) -> Dict:
        """
        Convertit les données client du format v2 au format v1 attendu par l'API.
        
        Args:
            v2_data: Données client au format v2
            
        Returns:
            Données client au format v1
        """
        # Extraction des données depuis la structure attendue
        third = v2_data.get("third", {})
        contact = v2_data.get("contact", {})
        address = v2_data.get("address", {})
        
        # Construction des données au format v1
        v1_data = {
            "third": third,
            "contact": contact,
            "address": address
        }
        
        return v1_data
    
    def test_authentication(self) -> bool:
        """
        Teste l'authentification en récupérant les informations sur l'API.
        
        Returns:
            True si l'authentification est réussie, False sinon
        """
        self.logger.info("🔄 Test d'authentification Sellsy...")
        
        try:
            # Appel à une méthode simple pour tester l'authentification
            request_settings = {
                "method": "Infos.getInfos"
            }
            
            response = self.request_api(request_settings)
            
            if response and response.get('status') == 'success':
                self.logger.info("✅ Authentification réussie!")
                return True
            else:
                self.logger.error("❌ Échec d'authentification")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors du test d'authentification: {str(e)}")
            return False
    
    def get_client(self, client_id: str) -> Optional[Dict]:
        """
        Récupère les informations d'un client par son ID.
        
        Args:
            client_id: ID du client à récupérer
            
        Returns:
            Informations du client ou None en cas d'erreur
        """
        self.logger.info(f"🔄 Récupération du client ID: {client_id}")
        
        try:
            # Préparation de la requête
            request_settings = {
                "method": "Client.getOne",
                "params": {
                    "clientid": client_id
                }
            }
            
            # Exécution de la requête
            response = self.request_api(request_settings)
            
            if response and response.get('status') == 'success':
                self.logger.info(f"✅ Client récupéré avec succès")
                return response
            else:
                self.logger.error(f"❌ Échec de récupération du client")
                return response
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la récupération du client: {str(e)}")
            return None
    
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
        
        try:
            # Préparation de la requête
            request_settings = {
                "method": "Client.getList",
                "params": {
                    "pagination": {
                        "nbperpage": limit,
                        "pagenum": 1
                    }
                }
            }
            
            # Ajout du terme de recherche si fourni
            if search_term:
                request_settings["params"]["search"] = {
                    "contains": search_term
                }
            
            # Exécution de la requête
            response = self.request_api(request_settings)
            
            if response and response.get('status') == 'success':
                result_count = len(response.get('response', {}).get('result', {}))
                self.logger.info(f"✅ {result_count} clients trouvés")
                return response
            else:
                self.logger.error(f"❌ Échec de la recherche de clients")
                return response
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la recherche de clients: {str(e)}")
            return None
