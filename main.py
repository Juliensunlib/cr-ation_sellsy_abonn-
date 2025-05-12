import os
import sys
import time
import requests
import logging
from logging.handlers import RotatingFileHandler
from typing import Dict, List, Optional
from datetime import datetime

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
    # Récupération des variables d'environnement
    AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
    AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
    AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME")
    
    # Ajout de logs de debug pour les variables
    logger.debug(f"AIRTABLE_API_KEY: {'présent' if AIRTABLE_API_KEY else 'absent'}")
    logger.debug(f"AIRTABLE_BASE_ID: {'présent' if AIRTABLE_BASE_ID else 'absent'}")
    logger.debug(f"AIRTABLE_TABLE_NAME: {'présent' if AIRTABLE_TABLE_NAME else 'absent'}")

class AirtableAPI:
    BASE_URL = f"https://api.airtable.com/v0/{Config.AIRTABLE_BASE_ID}/{Config.AIRTABLE_TABLE_NAME}"
    
    @staticmethod
    def get_records() -> List[Dict]:
        """Récupère tous les enregistrements d'Airtable."""
        logger.info("🔍 Début de récupération des enregistrements Airtable")
        logger.debug(f"URL de requête : {AirtableAPI.BASE_URL}")
        
        headers = {
            "Authorization": f"Bearer {Config.AIRTABLE_API_KEY}",
            "Content-Type": "application/json"
        }
        
        # Log détaillé de la requête
        logger.debug(f"Headers d'authentification : {headers}")
        
        records = []
        offset = None

        try:
            while True:
                params = {"offset": offset} if offset else {}
                
                logger.debug(f"Paramètres de requête : {params}")
                
                response = requests.get(
                    AirtableAPI.BASE_URL, 
                    headers=headers, 
                    params=params
                )
                
                # Log du code de réponse et du contenu
                logger.debug(f"Code de réponse : {response.status_code}")
                logger.debug(f"Contenu de la réponse : {response.text}")
                
                response.raise_for_status()
                data = response.json()
                
                page_records = data.get("records", [])
                records.extend(page_records)
                
                logger.info(f"📦 Récupéré {len(page_records)} enregistrements (Total: {len(records)})")
                
                offset = data.get("offset")
                if not offset:
                    break
            
            logger.info(f"✅ Récupération terminée. {len(records)} enregistrements au total.")
            return records
        
        except requests.RequestException as e:
            logger.error(f"❌ Erreur lors de la récupération des enregistrements Airtable : {e}")
            logger.error(f"Détails de l'erreur : {e.response.text if hasattr(e, 'response') else 'Pas de détails supplémentaires'}")
            return []

def main():
    """Fonction principale de synchronisation."""
    logger.info("🚀 Démarrage de la synchronisation des clients")
    
    # Vérification détaillée des configurations
    logger.info("📋 Vérification de la configuration")
    config_vars = [
        "AIRTABLE_API_KEY", 
        "AIRTABLE_BASE_ID", 
        "AIRTABLE_TABLE_NAME"
    ]
    
    for var in config_vars:
        value = os.environ.get(var)
        logger.info(f"{var}: {'✅ Présent' if value else '❌ Absent'}")
        if not value:
            logger.error(f"La variable {var} est manquante")
    
    # Récupération des enregistrements Airtable
    records = AirtableAPI.get_records()
    
    if not records:
        logger.info("⏹️ Aucun client à synchroniser.")

if __name__ == "__main__":
    main()
