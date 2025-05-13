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
        
        # Réinitialiser le résultat de synchronisation
        self.sync_result = None
        
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
                        # Stocker le résultat pour le wrapper
                        self.sync_result = {"id": client_id}
                        
                        # Mise à jour d'Airtable avec le nouvel ID Sellsy (sera remplacée par le wrapper)
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
        # Essayons différentes variantes possibles pour le nom du champ ID_Sellsy
        possible_id_fields = ["ID_Sellsy", "Id_Sellsy", "id_sellsy", "ID Sellsy", "Id Sellsy", "id sellsy"]
        
        # D'abord, récupérons un enregistrement pour examiner les noms de champs
        sample_records = synchronizer.airtable_api.get_records(None, 1)
        sellsy_id_field = None
        
        if sample_records and len(sample_records) > 0:
            sample_fields = sample_records[0].get('fields', {})
            logger.debug(f"Champs disponibles dans Airtable: {list(sample_fields.keys())}")
            
            # Vérifions quel champ est utilisé pour l'ID Sellsy
            for field in possible_id_fields:
                if field in sample_fields:
                    sellsy_id_field = field
                    logger.info(f"✓ Champ d'ID Sellsy identifié : {sellsy_id_field}")
                    break
        
        if not sellsy_id_field:
            logger.warning("⚠️ Impossible de déterminer le champ ID Sellsy. Utilisation par défaut: 'ID_Sellsy'")
            sellsy_id_field = "ID_Sellsy"
        
        # Maintenant, recherchons les enregistrements sans ID Sellsy
        filter_formula = f"BLANK({{{sellsy_id_field}}})"
        logger.info(f"🔍 Recherche des clients sans ID Sellsy avec la formule: {filter_formula}")
        
        try:
            # D'abord, récupérons tous les enregistrements pour voir combien il y en a
            all_records = synchronizer.airtable_api.get_records()
            logger.info(f"📊 Nombre total d'enregistrements dans Airtable: {len(all_records)}")
            
            # Puis, récupérons les enregistrements à synchroniser
            records_to_sync = synchronizer.airtable_api.get_records(filter_formula)
            
            logger.info(f"📝 Nombre d'enregistrements à synchroniser: {len(records_to_sync) if records_to_sync else 0}")
            
            # Affichons les premiers enregistrements pour débogage
            if records_to_sync and len(records_to_sync) > 0:
                logger.debug(f"Premier enregistrement à synchroniser: {json.dumps(records_to_sync[0].get('fields', {}))}")
                
                # Mise à jour de la fonction synchronize_client pour utiliser le bon champ
                original_sync_client = synchronizer.synchronize_client
                def sync_client_wrapper(record):
                    try:
                        original_sync_client(record)
                        # Si la synchronisation réussit, mettre à jour avec le bon nom de champ
                        if hasattr(synchronizer, 'sync_result') and synchronizer.sync_result:
                            client_id = synchronizer.sync_result.get('id')
                            if client_id:
                                synchronizer.airtable_api.update_record(record['id'], {sellsy_id_field: str(client_id)})
                    except Exception as e:
                        logger.error(f"❌ Erreur dans le wrapper de synchronisation: {str(e)}")
                
                # Remplacer temporairement la méthode
                original_method = synchronizer.synchronize_client
                synchronizer.synchronize_client = sync_client_wrapper
            
            if not records_to_sync:
                logger.info("⏹️ Aucun client sans ID Sellsy à synchroniser.")
                return
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération des enregistrements: {str(e)}")
            logger.exception("Détails de l'erreur:")
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
