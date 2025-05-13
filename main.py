import os
import sys
import time
import json
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional
from datetime import datetime
from dotenv import load_dotenv

# Import des classes API
from sellsy_api import SellsyAPI
from airtable_api import AirtableAPI

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
    
    # Utiliser les variables pour l'API Sellsy v1
    SELLSY_API_CONSUMER_TOKEN = os.environ.get("SELLSY_API_CONSUMER_TOKEN")
    SELLSY_API_CONSUMER_SECRET = os.environ.get("SELLSY_API_CONSUMER_SECRET")
    SELLSY_API_USER_TOKEN = os.environ.get("SELLSY_API_USER_TOKEN")
    SELLSY_API_USER_SECRET = os.environ.get("SELLSY_API_USER_SECRET")

class ClientSynchronizer:
    """Classe pour synchroniser les clients entre Airtable et Sellsy."""
    
    def __init__(self):
        """Initialise les API clients."""
        self.airtable_api = AirtableAPI(
            Config.AIRTABLE_API_KEY,
            Config.AIRTABLE_BASE_ID,
            Config.AIRTABLE_TABLE_NAME
        )
        
        # Initialisation avec l'API Sellsy v1
        self.sellsy_api = SellsyAPI(
            Config.SELLSY_API_CONSUMER_TOKEN,
            Config.SELLSY_API_CONSUMER_SECRET,
            Config.SELLSY_API_USER_TOKEN,
            Config.SELLSY_API_USER_SECRET,
            logger
        )
    
    def sanitize_client_data(self, record_fields: Dict) -> Optional[Dict]:
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
        
        # Format en suivant le format v2 pour être converti en format v1 dans l'API Sellsy
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

    def synchronize_client(self, record: Dict):
        """
        Synchronise un client d'Airtable vers Sellsy.
        
        Args:
            record: Enregistrement Airtable à synchroniser
        """
        record_fields = record.get('fields', {})
        logger.info(f"🔄 Début de synchronisation pour l'enregistrement : {record['id']}")
        
        # Préparation et validation des données
        formatted_data = self.sanitize_client_data(record_fields)
        
        if not formatted_data:
            logger.warning("⏩ Synchronisation ignorée pour cet enregistrement - données insuffisantes")
            return
        
        try:
            # Création du client dans Sellsy
            response = self.sellsy_api.create_client(formatted_data)

            if response:
                # Vérification de la réponse
                if response.get("status") == "success":
                    # Extraction de l'ID client
                    client_id = None
                    
                    if "response" in response and isinstance(response["response"], dict):
                        client_id = response["response"].get("id")
                    
                    if client_id:
                        logger.info(f"✅ Client créé avec succès dans Sellsy. ID: {client_id}")
                        # Mise à jour d'Airtable avec le nouvel ID Sellsy
                        update_result = self.airtable_api.update_record(record['id'], {'ID_Sellsy': str(client_id)})
                        
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
            
        # Initialisation du synchroniseur
        synchronizer = ClientSynchronizer()
        
        # Récupération des enregistrements à synchroniser
        filter_formula = "BLANK({ID_Sellsy})"
        records_to_sync = synchronizer.airtable_api.get_records(filter_formula)
        
        if not records_to_sync:
            logger.info("⏹️ Aucun client sans ID_Sellsy à synchroniser.")
            return
        
        logger.info(f"🔄 Synchronisation de {len(records_to_sync)} clients")
        
        # Synchronisation de chaque client
        for record in records_to_sync:
            synchronizer.synchronize_client(record)
            # Pause légère entre les requêtes pour respecter les limites d'API
            time.sleep(1)
        
        end_time = time.time()
        logger.info(f"✅ Synchronisation terminée en {end_time - start_time:.2f} secondes")
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution globale : {e}")
        logger.exception("Détails complets de l'erreur:")

if __name__ == "__main__":
    main()
