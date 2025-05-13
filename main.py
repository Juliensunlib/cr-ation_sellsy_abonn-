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

def setup_logging():
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
    AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
    AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME")
    
    SELLSY_API_CONSUMER_TOKEN = os.environ.get("SELLSY_API_CONSUMER_TOKEN")
    SELLSY_API_CONSUMER_SECRET = os.environ.get("SELLSY_API_CONSUMER_SECRET")
    SELLSY_API_USER_TOKEN = os.environ.get("SELLSY_API_USER_TOKEN")
    SELLSY_API_USER_SECRET = os.environ.get("SELLSY_API_USER_SECRET")

class AirtableAPI:
    BASE_URL = f"https://api.airtable.com/v0/{Config.AIRTABLE_BASE_ID}/{Config.AIRTABLE_TABLE_NAME}"
    
    @staticmethod
    def get_records(filter_formula=None) -> List[Dict]:
        """Récupère les enregistrements d'Airtable selon le filtre spécifié."""
        logger.info("🔍 Début de récupération des enregistrements Airtable")
        logger.debug(f"URL de requête : {AirtableAPI.BASE_URL}")
        
        headers = {
            "Authorization": f"Bearer {Config.AIRTABLE_API_KEY}",
            "Content-Type": "application/json"
        }
        
        records = []
        offset = None

        try:
            while True:
                params = {"offset": offset}
                
                # Ajout du filtre si spécifié
                if filter_formula:
                    params["filterByFormula"] = filter_formula
                
                logger.debug(f"Paramètres de requête : {params}")
                
                response = requests.get(
                    AirtableAPI.BASE_URL, 
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
            logger.error(f"Détails de l'erreur : {e.response.text if hasattr(e, 'response') else 'Pas de détails supplémentaires'}")
            return []

    @staticmethod
    def update_record(record_id: str, fields: Dict):
        """Met à jour un enregistrement dans Airtable."""
        headers = {
            "Authorization": f"Bearer {Config.AIRTABLE_API_KEY}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.patch(
                f"{AirtableAPI.BASE_URL}/{record_id}", 
                headers=headers, 
                json={"fields": fields}
            )
            response.raise_for_status()
            logger.info(f"🔄 Mise à jour de l'enregistrement {record_id} réussie")
            return response.json()
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la mise à jour de l'enregistrement {record_id} : {e}")
            return None

class SellsyAPI:
    BASE_URL = "https://api.sellsy.com/v2"
    LEGACY_URL = "https://api.sellsy.com/0"
    
    @staticmethod
    def make_request(method: str, params: Dict) -> Optional[Dict]:
        """Effectue une requête à l'API Sellsy."""
        logger.info(f"📤 Envoi de la requête Sellsy : {method}")
        
        # Préparation de la requête selon la documentation Sellsy
        oauth = OAuth1(
            Config.SELLSY_API_CONSUMER_TOKEN,
            Config.SELLSY_API_CONSUMER_SECRET,
            Config.SELLSY_API_USER_TOKEN,
            Config.SELLSY_API_USER_SECRET
        )
        
        # Format spécifique pour l'ancienne API Sellsy
        data = {
            'request': 1,
            'io_mode': 'json',
            'do_in': json.dumps({
                'method': method,
                'params': params
            })
        }
        
        try:
            # Utilisation de l'URL d'API legacy
            response = requests.post(
                f"{SellsyAPI.LEGACY_URL}/do/api/v2", 
                data=data,
                auth=oauth
            )
            
            # Vérification du statut de la réponse
            response.raise_for_status()
            
            # Log des détails de la réponse pour le débogage
            logger.debug(f"Status code: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            logger.debug(f"Response content: {response.text[:200]}...")  # Limitons à 200 caractères
            
            try:
                result = response.json()
                logger.debug(f"📥 Réponse Sellsy pour {method}: {result}")
                
                # Vérification du statut de l'API
                if isinstance(result, dict) and result.get("status") == "error":
                    logger.error(f"❌ Erreur API Sellsy: {result.get('error')}")
                
                return result
            except json.JSONDecodeError as e:
                logger.error(f"❌ Erreur de décodage JSON: {e}")
                logger.error(f"Contenu de la réponse: {response.text}")
                return None
            
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la requête Sellsy {method}: {e}")
            logger.error(f"Détails: {e.response.text if hasattr(e, 'response') and e.response else 'Pas de détails'}")
            return None

class ClientSynchronizer:
    @staticmethod
    def sanitize_client_data(record_fields: Dict) -> Optional[Dict]:
        """Nettoie et valide les données du client."""
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
        
        # Préparation des données client selon la documentation Sellsy
        # Format attendu par l'API pour Client.create
        client_data = {
            "name": f"{record_fields['Nom']} {record_fields['Prenom']}".strip(),
            "type": "person",  # Type person car il s'agit d'un particulier
            "email": str(record_fields["Email"]).strip(),
            "tel": str(record_fields["Téléphone"]).strip(),
        }
        
        # Préparation des données de contact
        contact_data = {
            "name": str(record_fields["Nom"]).strip(),
            "forename": str(record_fields["Prenom"]).strip(),
            "email": str(record_fields["Email"]).strip(),
            "tel": str(record_fields["Téléphone"]).strip(),
        }
        
        # Préparation des données d'adresse
        address_data = {
            "name": "Adresse principale",
            "part1": str(record_fields["Adresse complète"]).strip(),
            "zip": str(record_fields["Code postal"]).strip(),
            "town": str(record_fields["Ville"]).strip(),
            "countrycode": "FR"  # Par défaut France
        }
        
        # Vérification du format de l'email
        if "@" not in client_data["email"]:
            logger.warning(f"⚠️ Format d'email invalide: {client_data['email']}")
            return None
        
        logger.info(f"✅ Données client validées pour {contact_data['name']} {contact_data['forename']}")
        
        # Retourner les données formatées selon la structure attendue par l'API
        return {
            "third": client_data,
            "contact": contact_data,
            "address": address_data
        }

    @staticmethod
    def synchronize_client(record: Dict):
        """Synchronise un client d'Airtable vers Sellsy."""
        record_fields = record.get('fields', {})
        logger.info(f"🔄 Début de synchronisation pour l'enregistrement : {record['id']}")
        
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
                        client_id = response["response"].get("client_id")
                    
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

def main():
    """Fonction principale de synchronisation."""
    logger.info("🚀 Démarrage de la synchronisation des clients")
    start_time = time.time()
    
    try:
        # Vérification des configurations
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
