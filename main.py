import os
import sys
import time
import json
import requests
import logging
import urllib.parse
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional
from datetime import datetime
from requests_oauthlib import OAuth1
from dotenv import load_dotenv

# Charger les variables d'environnement depuis un fichier .env si présent
load_dotenv()

def setup_logging():
    """Configure et initialise le système de journalisation."""
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)
    log_filename = os.path.join(log_dir, f'sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    logger = logging.getLogger('SellsySynchronizer')
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = RotatingFileHandler(
        log_filename, 
        maxBytes=10*1024*1024,  
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

class Config:
    """Configuration de l'application à partir des variables d'environnement."""
    AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
    AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME")
    
    SELLSY_API_CONSUMER_TOKEN = os.environ.get("SELLSY_API_CONSUMER_TOKEN")
    SELLSY_API_CONSUMER_SECRET = os.environ.get("SELLSY_API_CONSUMER_SECRET")
    SELLSY_API_USER_TOKEN = os.environ.get("SELLSY_API_USER_TOKEN")
    SELLSY_API_USER_SECRET = os.environ.get("SELLSY_API_USER_SECRET")

class AirtableAPI:
    """API client pour Airtable."""
    
    @staticmethod
    def get_records(filter_formula=None) -> List[Dict]:
        """
        Récupère les enregistrements d'Airtable selon le filtre spécifié.
        
        Args:
            filter_formula: Formule de filtrage Airtable (ex: "BLANK({ID_Sellsy})")
            
        Returns:
            Liste des enregistrements
        """
        logger.info("🔍 Début de récupération des enregistrements Airtable")
        base_url = f"https://api.airtable.com/v0/{Config.AIRTABLE_BASE_ID}/{Config.AIRTABLE_TABLE_NAME}"
        logger.debug(f"URL de requête : {base_url}")
        
        headers = {
            "Authorization": f"Bearer {Config.AIRTABLE_API_KEY}",
            "Content-Type": "application/json"
        }
        
        records = []
        offset = None

        try:
            while True:
                params = {}
                if offset:
                    params["offset"] = offset
                
                # Ajout du filtre si spécifié
                if filter_formula:
                    params["filterByFormula"] = filter_formula
                
                logger.debug(f"Paramètres de requête : {params}")
                
                response = requests.get(
                    base_url, 
                    headers=headers, 
                    params=params
                )
                
                response.raise_for_status()
                data = response.json()
                
                page_records = data.get("records", [])
                records.extend(page_records)
                
                logger.info(f"📦 Récupéré {len(page_records)} enregistrements (Total: {len(records)})")
                
                offset = data.get("offset")
                if not offset:
                    break
            
            if filter_formula:
                logger.info(f"✅ Récupération terminée. {len(records)} enregistrements correspondant au filtre.")
            else:
                logger.info(f"✅ Récupération terminée. {len(records)} enregistrements au total.")
            return records
        
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la récupération des enregistrements Airtable : {e}")
            if hasattr(e, 'response'):
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Détails de l'erreur : {e.response.text}")
            return []

    @staticmethod
    def update_record(record_id: str, fields: Dict):
        """
        Met à jour un enregistrement dans Airtable.
        
        Args:
            record_id: ID de l'enregistrement à mettre à jour
            fields: Dictionnaire des champs à mettre à jour
            
        Returns:
            Réponse de l'API Airtable
        """
        base_url = f"https://api.airtable.com/v0/{Config.AIRTABLE_BASE_ID}/{Config.AIRTABLE_TABLE_NAME}"
        headers = {
            "Authorization": f"Bearer {Config.AIRTABLE_API_KEY}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.patch(
                f"{base_url}/{record_id}", 
                headers=headers, 
                json={"fields": fields}
            )
            response.raise_for_status()
            logger.info(f"🔄 Mise à jour de l'enregistrement {record_id} réussie")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la mise à jour de l'enregistrement {record_id} : {e}")
            if hasattr(e, 'response'):
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Détails de l'erreur : {e.response.text}")
            return None

class SellsyAPI:
    """API client pour Sellsy."""
    # CORRECTION: URL d'API corrigée pour correspondre à la documentation Sellsy
    API_URL = "https://api.sellsy.com/v2/oauth/authentication" # Pour vérifier l'authentification
    API_ENDPOINT = "https://api.sellsy.com/v2" # Base pour les requêtes API v2
    
    @staticmethod
    def make_request(method: str, params: Dict) -> Optional[Dict]:
        """
        Effectue une requête à l'API Sellsy.
        
        Args:
            method: Méthode Sellsy à appeler (ex: "Client.create")
            params: Paramètres de la méthode
            
        Returns:
            Réponse de l'API Sellsy ou None en cas d'erreur
        """
        logger.info(f"📤 Envoi de la requête Sellsy : {method}")
        
        # Vérification des informations d'authentification
        if not all([Config.SELLSY_API_CONSUMER_TOKEN, Config.SELLSY_API_CONSUMER_SECRET,
                    Config.SELLSY_API_USER_TOKEN, Config.SELLSY_API_USER_SECRET]):
            logger.error("❌ Informations d'authentification Sellsy incomplètes")
            return None
        
        # Préparation de l'authentification OAuth1
        oauth = OAuth1(
            Config.SELLSY_API_CONSUMER_TOKEN,
            Config.SELLSY_API_CONSUMER_SECRET,
            Config.SELLSY_API_USER_TOKEN,
            Config.SELLSY_API_USER_SECRET
        )
        
        try:
            # Tester l'authentification d'abord
            auth_check = requests.get(SellsyAPI.API_URL, auth=oauth)
            
            if auth_check.status_code != 200:
                logger.error(f"❌ Échec de l'authentification Sellsy: {auth_check.status_code}")
                logger.error(f"Détails: {auth_check.text}")
                return None
            
            logger.info("✅ Authentification Sellsy réussie")
            
            # CORRECTION: Utilisation de l'API Sellsy v2 (REST) au lieu de v1
            if method == "Client.create":
                endpoint = f"{SellsyAPI.API_ENDPOINT}/contacts"
                response = requests.post(endpoint, json=params, auth=oauth)
            elif method.startswith("Client."):
                # Adapter selon les méthodes nécessaires
                client_id = params.get("clientid", "")
                endpoint = f"{SellsyAPI.API_ENDPOINT}/contacts/{client_id}"
                response = requests.get(endpoint, auth=oauth)
            else:
                logger.error(f"❌ Méthode non supportée: {method}")
                return None
            
            # Log de la requête pour débogage
            logger.debug(f"URL: {endpoint if 'endpoint' in locals() else 'non définie'}")
            logger.debug(f"Méthode: {method}")
            logger.debug(f"Paramètres: {json.dumps(params)}")
            
            # Vérification du statut de la réponse HTTP
            response.raise_for_status()
            
            # Log des détails de la réponse pour débogage
            logger.debug(f"Status code: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            logger.debug(f"Response content (début): {response.text[:200]}...")
            
            try:
                # Tentative de décodage JSON
                result = response.json()
                logger.info(f"✅ Réponse Sellsy réussie pour {method}")
                return {"status": "success", "response": result}
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ Erreur de décodage JSON: {e}")
                logger.error(f"Contenu de la réponse: {response.text}")
                return None
            
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la requête Sellsy {method}: {e}")
            if hasattr(e, 'response') and e.response:
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Détails: {e.response.text}")
            return None

class ClientSynchronizer:
    """Classe pour synchroniser les clients entre Airtable et Sellsy."""
    
    @staticmethod
    def sanitize_client_data(record_fields: Dict) -> Optional[Dict]:
        """
        Nettoie et valide les données du client avant l'envoi à Sellsy.
        
        Args:
            record_fields: Champs de l'enregistrement Airtable
            
        Returns:
            Données client formatées pour Sellsy ou None si données invalides
        """
        required_fields = [
            'Nom', 'Prenom', 'Email', 'Téléphone', 
            'Adresse complète', 'Code postal', 'Ville'
        ]
        
        # Vérifie que tous les champs requis sont présents et non vides
        missing_fields = []
        for field in required_fields:
            if field not in record_fields or not record_fields[field]:
                missing_fields.append(field)
        
        if missing_fields:
            logger.warning(f"⚠️ Champs manquants ou vides : {', '.join(missing_fields)}")
            return None
        
        # Nettoyage des données
        nom = str(record_fields["Nom"]).strip()
        prenom = str(record_fields["Prenom"]).strip()
        email = str(record_fields["Email"]).strip()
        telephone = str(record_fields["Téléphone"]).strip()
        adresse = str(record_fields["Adresse complète"]).strip()
        code_postal = str(record_fields["Code postal"]).strip()
        ville = str(record_fields["Ville"]).strip()
        
        # Vérification du format de l'email
        if "@" not in email:
            logger.warning(f"⚠️ Format d'email invalide: {email}")
            return None
        
        # CORRECTION: Adaptation du format pour l'API Sellsy v2
        client_data = {
            "type": "person",
            "name": f"{nom} {prenom}",
            "email": email,
            "phone": telephone,
            "contact": {
                "name": nom,
                "firstName": prenom,
                "email": email,
                "mobile": telephone
            },
            "address": {
                "name": "Adresse principale",
                "address": adresse,
                "zipcode": code_postal,
                "city": ville,
                "countryCode": "FR"  # Par défaut France
            }
        }
        
        logger.info(f"✅ Données client validées pour {nom} {prenom}")
        return client_data

    @staticmethod
    def synchronize_client(record: Dict):
        """
        Synchronise un client d'Airtable vers Sellsy.
        
        Args:
            record: Enregistrement Airtable à synchroniser
        """
        record_fields = record.get('fields', {})
        logger.info(f"🔄 Début de synchronisation pour l'enregistrement : {record['id']}")
        
        # Préparation et validation des données
        formatted_data = ClientSynchronizer.sanitize_client_data(record_fields)
        
        if not formatted_data:
            logger.warning("⏩ Synchronisation ignorée pour cet enregistrement - données insuffisantes")
            return
        
        try:
            # Création du client dans Sellsy
            response = SellsyAPI.make_request("Client.create", formatted_data)

            if response:
                # Vérification de la réponse
                if response.get("status") == "success":
                    # Extraction de l'ID client selon la structure de la réponse
                    client_id = None
                    
                    if "response" in response and isinstance(response["response"], dict):
                        client_id = response["response"].get("id")
                    
                    if client_id:
                        logger.info(f"✅ Client créé avec succès dans Sellsy. ID: {client_id}")
                        # Mise à jour d'Airtable avec le nouvel ID Sellsy
                        update_result = AirtableAPI.update_record(record['id'], {'ID_Sellsy': str(client_id)})
                        
                        if update_result:
                            logger.info(f"✅ Mise à jour de l'ID Sellsy dans Airtable réussie")
                        else:
                            logger.error(f"❌ Échec de la mise à jour de l'ID Sellsy dans Airtable")
                    else:
                        logger.error(f"❌ Impossible de trouver l'ID client dans la réponse: {response}")
                else:
                    error_msg = response.get("error", "Réponse inconnue")
                    logger.error(f"🚨 Échec de la synchronisation du client: {error_msg}")
            else:
                logger.error("🚨 Pas de réponse valide de l'API Sellsy")
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la synchronisation : {str(e)}")
            logger.exception("Détails de l'erreur:")

def check_configuration():
    """Vérifie que toute la configuration nécessaire est présente."""
    missing_configs = []
    
    if not Config.AIRTABLE_API_KEY:
        missing_configs.append("AIRTABLE_API_KEY")
    if not Config.AIRTABLE_BASE_ID:
        missing_configs.append("AIRTABLE_BASE_ID")
    if not Config.AIRTABLE_TABLE_NAME:
        missing_configs.append("AIRTABLE_TABLE_NAME")
    if not Config.SELLSY_API_CONSUMER_TOKEN:
        missing_configs.append("SELLSY_API_CONSUMER_TOKEN")
    if not Config.SELLSY_API_CONSUMER_SECRET:
        missing_configs.append("SELLSY_API_CONSUMER_SECRET")
    if not Config.SELLSY_API_USER_TOKEN:
        missing_configs.append("SELLSY_API_USER_TOKEN")
    if not Config.SELLSY_API_USER_SECRET:
        missing_configs.append("SELLSY_API_USER_SECRET")
        
    if missing_configs:
        logger.error(f"❌ Configuration incomplète. Variables manquantes: {', '.join(missing_configs)}")
        return False
        
    # Afficher les premières lettres des tokens pour le débogage (sans révéler les secrets)
    if Config.SELLSY_API_CONSUMER_TOKEN:
        logger.debug(f"SELLSY_API_CONSUMER_TOKEN: {Config.SELLSY_API_CONSUMER_TOKEN[:3]}...")
    if Config.SELLSY_API_USER_TOKEN:
        logger.debug(f"SELLSY_API_USER_TOKEN: {Config.SELLSY_API_USER_TOKEN[:3]}...")
    
    return True

def main():
    """Fonction principale de synchronisation."""
    logger.info("🚀 Démarrage de la synchronisation des clients")
    start_time = time.time()
    
    try:
        # Vérification des configurations
        if not check_configuration():
            return
            
        # Récupération de tous les enregistrements pour diagnostic
        all_records = AirtableAPI.get_records()
        
        if not all_records:
            logger.error("❌ Impossible de récupérer les enregistrements Airtable")
            return
            
        logger.info(f"📊 Total des enregistrements dans Airtable: {len(all_records)}")
        
        # Filtrage manuel des enregistrements sans ID_Sellsy
        records_to_sync = []
        for record in all_records:
            fields = record.get('fields', {})
            if 'ID_Sellsy' not in fields or not fields['ID_Sellsy']:
                records_to_sync.append(record)
                full_name = f"{fields.get('Nom', 'Sans nom')} {fields.get('Prenom', 'Sans prénom')}"
                logger.info(f"🔍 Trouvé un enregistrement sans ID_Sellsy: {record['id']} - {full_name}")
        
        if not records_to_sync:
            logger.info("⏹️ Aucun client sans ID_Sellsy à synchroniser.")
            
            # Afficher les champs disponibles dans le premier enregistrement pour diagnostic
            if all_records:
                sample_record = all_records[0]
                logger.info(f"📋 Exemple de champs disponibles dans un enregistrement: {list(sample_record.get('fields', {}).keys())}")
                
                # Vérifier si le champ existe avec une orthographe différente
                possible_id_fields = [field for field in sample_record.get('fields', {}).keys() 
                                    if 'id' in field.lower() and 'sellsy' in field.lower()]
                if possible_id_fields:
                    logger.info(f"💡 Champs potentiellement liés à Sellsy trouvés: {possible_id_fields}")
            
            return
        
        logger.info(f"🔄 Synchronisation de {len(records_to_sync)} clients")
        
        # Synchronisation de chaque client
        for record in records_to_sync:
            ClientSynchronizer.synchronize_client(record)
            # Pause légère entre les requêtes pour respecter les limites d'API
            time.sleep(1)
        
        end_time = time.time()
        logger.info(f"✅ Synchronisation terminée en {end_time - start_time:.2f} secondes")
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution globale : {e}")
        logger.exception("Détails complets de l'erreur:")

if __name__ == "__main__":
    main()
