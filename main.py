import os
import time
import requests
import logging
from typing import Dict, List, Optional

# Configuration du logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration des variables d'environnement
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
        headers = {"Authorization": f"Bearer {Config.AIRTABLE_API_KEY}"}
        records = []
        offset = None

        while True:
            params = {"offset": offset} if offset else {}
            
            try:
                response = requests.get(
                    AirtableAPI.BASE_URL, 
                    headers=headers, 
                    params=params
                )
                response.raise_for_status()
                data = response.json()
                
                records.extend(data.get("records", []))
                
                offset = data.get("offset")
                if not offset:
                    break
            
            except requests.RequestException as e:
                logger.error(f"Erreur lors de la récupération des enregistrements Airtable : {e}")
                break
        
        return records

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
            logger.info(f"Mise à jour de l'enregistrement {record_id} réussie")
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la mise à jour de l'enregistrement {record_id} : {e}")

class SellsyAPI:
    BASE_URL = "https://api.sellsy.com/0/"
    
    @staticmethod
    def make_request(method: str, params: Dict) -> Optional[Dict]:
        """Effectue une requête à l'API Sellsy."""
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
            return response.json()
        
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la requête Sellsy {method}: {e}")
            return None

    @staticmethod
    def search_client(email: str) -> Optional[Dict]:
        """Recherche un client par email."""
        response = SellsyAPI.make_request("Client.search", {
            "search": {"email": email}
        })
        
        if response and response.get('response', {}).get('total', 0) > 0:
            return response['response']['list'][0]
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
        if not all(field in record_fields for field in required_fields):
            logger.warning(f"Enregistrement incomplet. Champs manquants.")
            return None
        
        # Vérifie si un ID Sellsy existe déjà
        if record_fields.get('ID_Sellsy'):
            logger.info(f"Client {record_fields['Nom']} {record_fields['Prenom']} déjà synchronisé.")
            return None
        
        return {
            "name": record_fields["Nom"],
            "forename": record_fields["Prenom"],
            "email": record_fields["Email"],
            "tel": record_fields["Téléphone"],
            "address": record_fields["Adresse complète"],
            "zip": record_fields["Code postal"],
            "town": record_fields["Ville"]
        }

    @staticmethod
    def synchronize_client(record: Dict):
        """Synchronise un client d'Airtable vers Sellsy."""
        record_fields = record.get('fields', {})
        client_data = ClientSynchronizer.sanitize_client_data(record_fields)
        
        if not client_data:
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
                logger.info(f"Client {action} avec succès. ID Sellsy : {client_id}")
                AirtableAPI.update_record(record['id'], {'ID_Sellsy': client_id})
            else:
                logger.error("Impossible de synchroniser le client.")
        
        except Exception as e:
            logger.error(f"Erreur lors de la synchronisation : {e}")

def main():
    """Fonction principale de synchronisation."""
    logger.info("🚀 Démarrage de la synchronisation des clients")
    
    try:
        # Récupération des enregistrements Airtable
        records = AirtableAPI.get_records()
        
        if not records:
            logger.info("Aucun client à synchroniser.")
            return
        
        logger.info(f"Synchronisation de {len(records)} clients")
        
        # Synchronisation de chaque client
        for record in records:
            ClientSynchronizer.synchronize_client(record)
        
        logger.info("✅ Synchronisation terminée")
    
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution globale : {e}")

if __name__ == "__main__":
    main()
