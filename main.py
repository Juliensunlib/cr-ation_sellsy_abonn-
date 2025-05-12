import os
import sys
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional
from datetime import datetime

# Configuration avancée du logging
def setup_logging():
    # Créer le répertoire de logs s'il n'existe pas
    log_dir = 'logs'
    os.makedirs(log_dir, exist_ok=True)

    # Nom de fichier de log avec date et heure
    log_filename = os.path.join(log_dir, f'sync_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')

    # Configuration du logger
    logger = logging.getLogger('SellsySynchronizer')
    logger.setLevel(logging.DEBUG)

    # Formatter pour les logs détaillés
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Handler de fichier avec rotation
    file_handler = RotatingFileHandler(
        log_filename, 
        maxBytes=10*1024*1024,  # 10 Mo
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # Handler de console
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # Ajouter les handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Initialisation du logger
logger = setup_logging()

# Reste du code précédent (Config, AirtableAPI, SellsyAPI, etc.)
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
    def get_records() -> List[Dict]:
        """Récupère tous les enregistrements d'Airtable."""
        logger.info("🔍 Début de récupération des enregistrements Airtable")
        headers = {"Authorization": f"Bearer {Config.AIRTABLE_API_KEY}"}
        records = []
        offset = None
        total_records = 0

        try:
            while True:
                params = {"offset": offset} if offset else {}
                
                response = requests.get(
                    AirtableAPI.BASE_URL, 
                    headers=headers, 
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                
                page_records = data.get("records", [])
                records.extend(page_records)
                total_records += len(page_records)
                
                logger.info(f"📦 Récupéré {len(page_records)} enregistrements (Total: {total_records})")
                
                offset = data.get("offset")
                if not offset:
                    break
            
            logger.info(f"✅ Récupération terminée. {total_records} enregistrements au total.")
            return records
        
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la récupération des enregistrements Airtable : {e}")
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
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la mise à jour de l'enregistrement {record_id} : {e}")

class SellsyAPI:
    BASE_URL = "https://api.sellsy.com/0/"
    
    @staticmethod
    def make_request(method: str, params: Dict) -> Optional[Dict]:
        """Effectue une requête à l'API Sellsy."""
        logger.info(f"📤 Envoi de la requête Sellsy : {method}")
        
        headers = {
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.post(
                SellsyAPI.BASE_URL, 
                json={
                    "method": method,
                    "params": params
                },
                auth=(
                    Config.SELLSY_API_CONSUMER_TOKEN, 
                    Config.SELLSY_API_USER_TOKEN
                ),
                headers=headers
            )
            response.raise_for_status()
            
            result = response.json()
            logger.debug(f"📥 Réponse Sellsy pour {method}: {result}")
            return result
        
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la requête Sellsy {method}: {e}")
            return None

    @staticmethod
    def search_client(email: str) -> Optional[Dict]:
        """Recherche un client par email."""
        logger.info(f"🔍 Recherche du client avec l'email : {email}")
        response = SellsyAPI.make_request("Client.search", {
            "search": {"email": email}
        })
        
        if response and response.get('response', {}).get('total', 0) > 0:
            client = response['response']['list'][0]
            logger.info(f"✅ Client trouvé : {client.get('name', 'Nom inconnu')}")
            return client
        
        logger.info("❌ Aucun client trouvé avec cet email")
        return None

class ClientSynchronizer:
    @staticmethod
    def sanitize_client_data(record_fields: Dict) -> Optional[Dict]:
        """Nettoie et valide les données du client."""
        required_fields = [
            'Nom', 'Prenom', 'Email', 'Téléphone', 
            'Adresse complète', 'Code postal', 'Ville'
        ]
        
        # Vérifie que tous les champs requis sont présents
        missing_fields = [field for field in required_fields if field not in record_fields]
        if missing_fields:
            logger.warning(f"⚠️ Champs manquants : {', '.join(missing_fields)}")
            return None
        
        # Vérifie si un ID Sellsy existe déjà
        if record_fields.get('ID_Sellsy'):
            logger.info(f"✅ Client {record_fields['Nom']} {record_fields['Prenom']} déjà synchronisé.")
            return None
        
        client_data = {
            "name": record_fields["Nom"],
            "forename": record_fields["Prenom"],
            "email": record_fields["Email"],
            "tel": record_fields["Téléphone"],
            "address": record_fields["Adresse complète"],
            "zip": record_fields["Code postal"],
            "town": record_fields["Ville"]
        }
        
        logger.info(f"✅ Données client validées pour {client_data['name']} {client_data['forename']}")
        return client_data

    @staticmethod
    def synchronize_client(record: Dict):
        """Synchronise un client d'Airtable vers Sellsy."""
        record_fields = record.get('fields', {})
        logger.info(f"🔄 Début de synchronisation pour l'enregistrement : {record['id']}")
        
        client_data = ClientSynchronizer.sanitize_client_data(record_fields)
        
        if not client_data:
            logger.warning("⏩ Synchronisation ignorée pour cet enregistrement")
            return
        
        # Recherche d'un client existant
        existing_client = SellsyAPI.search_client(client_data['email'])
        
        try:
            if existing_client:
                # Mise à jour du client existant
                response = SellsyAPI.make_request("Client.update", {
                    "id": existing_client['id'],
                    "third": client_data
                })
                client_id = existing_client['id']
                action = "mis à jour"
            else:
                # Création d'un nouveau client
                response = SellsyAPI.make_request("Client.create", {"third": client_data})
                client_id = response.get('response', {}).get('client_id') if response else None
                action = "créé"
            
            # Mise à jour d'Airtable avec l'ID Sellsy
            if client_id:
                logger.info(f"✅ Client {action} avec succès. ID Sellsy : {client_id}")
                AirtableAPI.update_record(record['id'], {'ID_Sellsy': client_id})
            else:
                logger.error("❌ Impossible de synchroniser le client.")
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la synchronisation : {e}")

def main():
    """Fonction principale de synchronisation."""
    logger.info("🚀 Démarrage de la synchronisation des clients")
    start_time = time.time()
    
    try:
        # Vérification des configurations
        if not all([
            Config.AIRTABLE_API_KEY, Config.AIRTABLE_BASE_ID, Config.AIRTABLE_TABLE_NAME,
            Config.SELLSY_API_CONSUMER_TOKEN, Config.SELLSY_API_CONSUMER_SECRET,
            Config.SELLSY_API_USER_TOKEN, Config.SELLSY_API_USER_SECRET
        ]):
            logger.error("❌ Configuration incomplète. Vérifiez vos variables d'environnement.")
            return
        
        # Récupération des enregistrements Airtable
        records = AirtableAPI.get_records()
        
        if not records:
            logger.info("⏹️ Aucun client à synchroniser.")
            return
        
        logger.info(f"🔄 Synchronisation de {len(records)} clients")
        
        # Synchronisation de chaque client
        for record in records:
            ClientSynchronizer.synchronize_client(record)
        
        end_time = time.time()
        logger.info(f"✅ Synchronisation terminée en {end_time - start_time:.2f} secondes")
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution globale : {e}")

if __name__ == "__main__":
    main()
