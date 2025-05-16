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
    AUTH_URL = "https://login.sellsy.com/oauth2/access-tokens"
    
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
            
            # Préparation du payload pour la requête
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
            
            # En-têtes pour spécifier le type de contenu
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            # Debug pour voir ce qui est envoyé
            self.logger.debug(f"Envoi de requête d'authentification à {self.AUTH_URL}")
            self.logger.debug(f"Payload: {payload}")
            
            # Requête avec gestion explicite des timeouts et vérification SSL
            response = requests.post(
                self.AUTH_URL, 
                json=payload,
                headers=headers,
                timeout=30,
                verify=True
            )
            
            # Log de la réponse brute pour diagnostiquer les problèmes
            self.logger.debug(f"Code de statut de réponse: {response.status_code}")
            self.logger.debug(f"Réponse brute: {response.text[:200]}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    self.access_token = data["access_token"]
                    # Le refresh token n'est pas fourni avec client_credentials
                    # Calcul de la date d'expiration
                    expires_in = data.get("expires_in", 3600)
                    self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)  # -60 pour marge de sécurité
                    
                    self.logger.info(f"✅ Token d'accès obtenu avec succès (expire dans {expires_in} secondes)")
                    return True
                except json.JSONDecodeError as json_err:
                    self.logger.error(f"❌ Impossible de décoder la réponse JSON: {json_err}")
                    self.logger.error(f"Contenu de la réponse: {response.text}")
                    return False
            else:
                self.logger.error(f"❌ Échec d'obtention du token: {response.status_code}")
                self.logger.error(f"Détails: {response.text}")
                
                # Vérification spécifique pour les erreurs courantes
                if response.status_code == 401:
                    self.logger.error("❌ Authentification refusée. Vérifiez vos identifiants client_id et client_secret.")
                elif response.status_code == 400:
                    self.logger.error("❌ Requête incorrecte. Vérifiez le format des paramètres.")
                
                return False
                
        except requests.exceptions.Timeout:
            self.logger.error("❌ Timeout lors de la connexion à l'API Sellsy")
            return False
        except requests.exceptions.ConnectionError:
            self.logger.error("❌ Impossible de se connecter à l'API Sellsy - Vérifiez votre connexion internet")
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
                
                payload = {
                    "grant_type": "refresh_token",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "refresh_token": self.refresh_token
                }
                
                headers = {
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                }
                
                response = requests.post(
                    self.AUTH_URL, 
                    json=payload,
                    headers=headers,
                    timeout=30
                )
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        self.access_token = data["access_token"]
                        if "refresh_token" in data:
                            self.refresh_token = data["refresh_token"]
                        expires_in = data.get("expires_in", 3600)
                        self.token_expires_at = datetime.now() + timedelta(seconds=expires_in - 60)
                        
                        self.logger.info(f"✅ Token d'accès rafraîchi avec succès")
                        return True
                    except json.JSONDecodeError:
                        self.logger.error(f"❌ Impossible de décoder la réponse JSON: {response.text}")
                        return False
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
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
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
                
                # Exécution de la requête avec timeout
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    json=data if data else None,
                    params=params if params else None,
                    timeout=30
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
                        # On réessaie la requête avec le nouveau token lors de la prochaine itération
                        retry_count += 1
                        continue
                    return None
                else:
                    self.logger.error(f"❌ Erreur HTTP: {response.status_code}")
                    self.logger.error(f"Détails: {response.text}")
                    
                    # Si c'est une erreur temporaire (429, 500, 502, 503, 504), on réessaie
                    if response.status_code in [429, 500, 502, 503, 504]:
                        retry_count += 1
                        wait_time = 2 ** retry_count  # Backoff exponentiel
                        self.logger.info(f"⏱️ Attente de {wait_time} secondes avant nouvelle tentative ({retry_count}/{max_retries})")
                        time.sleep(wait_time)
                        continue
                    
                    return None
                
            except requests.exceptions.Timeout:
                self.logger.warning(f"⚠️ Timeout lors de la requête (tentative {retry_count+1}/{max_retries})")
                retry_count += 1
                if retry_count < max_retries:
                    time.sleep(2 ** retry_count)  # Backoff exponentiel
                    continue
                self.logger.error("❌ Échec après plusieurs tentatives (timeout)")
                return None
            except requests.RequestException as e:
                self.logger.error(f"❌ Erreur de connexion: {str(e)}")
                return None
            except Exception as e:
                self.logger.error(f"❌ Erreur inattendue: {str(e)}")
                self.logger.exception("Détails:")
                return None
            
            # Si on arrive ici, c'est qu'on a une réponse correcte ou une erreur définitive
            break
        
        return None  # En cas d'échec après toutes les tentatives
    
    def test_authentication(self) -> bool:
        """
        Teste l'authentification en utilisant un endpoint valide de l'API Sellsy V2.
        
        Returns:
            True si l'authentification est réussie, False sinon
        """
        self.logger.info("🔄 Test d'authentification Sellsy v2...")
        
        try:
            # Obtenons d'abord un token valide
            if not self.get_access_token():
                self.logger.error("❌ Impossible d'obtenir un token valide")
                return False
            
            # L'endpoint /myself n'existe pas dans l'API v2
            # À la place, on utilise un endpoint qui existe sûrement dans l'API v2 comme /individuals ou /companies avec une limite à 1
            response = self.request_api("GET", "/individuals", params={"limit": 1})
            
            if response is not None:
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
            # Déterminer le type de client (particulier ou entreprise)
            is_individual = client_data.get("third", {}).get("type") == "person"
            
            # Conversion du format des données pour l'API v2
            v2_client_data = self._prepare_client_data_for_v2(client_data, is_individual)
            
            # API v2 utilise différents endpoints pour individus et entreprises
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
                
                # Si des adresses ont été fournies mais n'ont pas été incluses dans la création initiale
                # (par exemple pour les entreprises où les adresses doivent être ajoutées séparément)
                if client_id and "addresses" in v2_client_data and not is_individual:
                    self._create_client_addresses(client_id, v2_client_data["addresses"], is_individual)
                
                return {"status": "success", "client_id": client_id, "response": response}
            else:
                self.logger.error("❌ Échec de création du client")
                return {"status": "error", "error": "Échec de création du client"}
                
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la création du client: {str(e)}")
            self.logger.exception("Détails:")
            return {"status": "error", "error": str(e)}
    
    def _create_client_addresses(self, client_id: str, addresses: List[Dict], is_individual: bool = False) -> bool:
        """
        Crée des adresses pour un client existant.
        
        Args:
            client_id: ID du client
            addresses: Liste des adresses à créer
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            True si la création a réussi, False sinon
        """
        if not addresses:
            return True
        
        entity_type = "individuals" if is_individual else "companies"
        endpoint = f"/{entity_type}/{client_id}/addresses"
        
        for address in addresses:
            try:
                response = self.request_api("POST", endpoint, address)
                if not response:
                    self.logger.error(f"❌ Échec de création d'adresse pour le client {client_id}")
                    return False
                self.logger.info(f"✅ Adresse créée avec succès pour le client {client_id}")
            except Exception as e:
                self.logger.error(f"❌ Erreur lors de la création d'adresse: {str(e)}")
                return False
        
        return True
    
    def _prepare_client_data_for_v2(self, old_data: Dict, is_individual: bool) -> Dict:
        """
        Convertit les données client du format v1 au format v2 attendu par l'API.
        
        Args:
            old_data: Données client au format v1
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            Données client au format v2
        """
        # Extraction des données de l'ancien format
        third = old_data.get("third", {})
        contact = old_data.get("contact", {})
        address = old_data.get("address", {})
        
        # Formatage en fonction du type de client
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
                "type": "client"  # Type du client (prospect/client)
            }
            
            # Pour les particuliers, les adresses sont incluses directement
            if address:
                address_data = self._format_address_for_v2(address)
                result["addresses"] = [address_data]
                
        else:
            # Format pour les entreprises (companies)
            result = {
                "name": third.get("name", ""),
                "email": third.get("email", ""),
                "phone_number": third.get("tel", ""),
                "note": third.get("notes", ""),
                "type": "client"  # Type du client (prospect/client)
            }
            
            # Pour les entreprises, on prépare les adresses mais elles seront ajoutées séparément après la création
            if address:
                address_data = self._format_address_for_v2(address)
                result["addresses"] = [address_data]
            
            # Ajout du contact pour les entreprises
            if contact:
                contact_data = {
                    "first_name": contact.get("firstname", ""),
                    "last_name": contact.get("name", ""),
                    "email": contact.get("email", ""),
                    "phone_number": contact.get("tel", ""),
                    "mobile_number": contact.get("mobile", ""),
                    "position": contact.get("position", ""),
                    "civil": {
                        "civil": "mr" if contact.get("civility") == "man" else "mrs"
                    }
                }
                result["contacts"] = [contact_data]
        
        self.logger.debug(f"Données client formatées pour v2: {result}")
        return result
    
    def _format_address_for_v2(self, address: Dict) -> Dict:
        """
        Formate une adresse selon le format attendu par l'API v2.
        
        Args:
            address: Données d'adresse au format v1
            
        Returns:
            Données d'adresse au format v2
        """
        return {
            "name": address.get("name", "Adresse principale"),
            "address_line_1": address.get("part1", ""),
            "address_line_2": address.get("part2", ""),
            "postal_code": address.get("zip", ""),
            "city": address.get("town", ""),
            "country": {
                "code": address.get("countrycode", "FR")
            },
            "is_invoicing_address": True,
            "is_delivery_address": True,
            "is_main": True
        }
    
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
    
    def update_client(self, client_id: str, client_data: Dict, is_individual: bool = False) -> Optional[Dict]:
        """
        Met à jour un client existant dans Sellsy.
        
        Args:
            client_id: ID du client à mettre à jour
            client_data: Nouvelles données du client
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        self.logger.info(f"🔄 Mise à jour du client ID: {client_id}")
        
        # Conversion du format des données pour l'API v2
        v2_client_data = self._prepare_client_data_for_v2(client_data, is_individual)
        
        # Déterminer l'endpoint en fonction du type de client
        endpoint = f"/individuals/{client_id}" if is_individual else f"/companies/{client_id}"
        
        # Exécuter la requête
        response = self.request_api("PUT", endpoint, v2_client_data)
        
        if response:
            self.logger.info(f"✅ Client mis à jour avec succès!")
            return {"status": "success", "response": response}
        else:
            self.logger.error("❌ Échec de mise à jour du client")
            return {"status": "error", "error": "Échec de mise à jour du client"}
    
    def search_clients(self, search_term=None, limit=100, offset=0, type_filter=None) -> Optional[Dict]:
        """
        Recherche des clients dans Sellsy.
        
        Args:
            search_term: Terme de recherche (facultatif)
            limit: Nombre maximum de résultats à retourner
            offset: Position de départ pour la pagination
            type_filter: Filtrer par type (client, prospect, etc.)
            
        Returns:
            Liste des clients ou None en cas d'erreur
        """
        self.logger.info(f"🔄 Recherche de clients" + (f" avec terme: {search_term}" if search_term else ""))
        
        # Paramètres de recherche
        params = {
            "limit": limit,
            "offset": offset
        }
        
        if search_term:
            params["search"] = search_term
            
        if type_filter:
            params["type"] = type_filter
        
        # Recherche dans les entreprises et particuliers
        companies = self.request_api("GET", "/companies", params=params) or {"data": []}
        individuals = self.request_api("GET", "/individuals", params=params) or {"data": []}
        
        # Combiner les résultats
        results = []
        
        if "data" in companies:
            for company in companies.get("data", []):
                company["client_type"] = "company"
                results.append(company)
                
        if "data" in individuals:
            for individual in individuals.get("data", []):
                individual["client_type"] = "individual"
                results.append(individual)
        
        # Trier et limiter les résultats
        results = results[:limit]
        
        return {"data": results, "total_count": len(results)}
    
    def create_address(self, client_id: str, address_data: Dict, is_individual: bool = False) -> Optional[Dict]:
        """
        Crée une adresse pour un client existant.
        
        Args:
            client_id: ID du client
            address_data: Données de l'adresse à créer
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            Réponse de l'API ou None en cas d'erreur
        """
        entity_type = "individuals" if is_individual else "companies"
        endpoint = f"/{entity_type}/{client_id}/addresses"
        
        # Formatage de l'adresse pour l'API v2
        v2_address_data = self._format_address_for_v2(address_data)
        
        self.logger.info(f"🔄 Création d'une adresse pour le client ID: {client_id}")
        return self.request_api("POST", endpoint, v2_address_data)
    
    def get_client_addresses(self, client_id: str, is_individual: bool = False) -> Optional[Dict]:
        """
        Récupère les adresses d'un client.
        
        Args:
            client_id: ID du client
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            Liste des adresses ou None en cas d'erreur
        """
        entity_type = "individuals" if is_individual else "companies"
        endpoint = f"/{entity_type}/{client_id}/addresses"
        
        self.logger.info(f"🔄 Récupération des adresses du client ID: {client_id}")
        return self.request_api("GET", endpoint)
