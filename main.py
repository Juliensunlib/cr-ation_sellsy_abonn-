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
    
    # Variables pour l'API Sellsy V2 (OAuth 2.0)
    SELLSY_CLIENT_ID = os.environ.get("SELLSY_CLIENT_ID")
    SELLSY_CLIENT_SECRET = os.environ.get("SELLSY_CLIENT_SECRET")
    SELLSY_ACCESS_TOKEN = os.environ.get("SELLSY_ACCESS_TOKEN", None)  # Optionnel
    SELLSY_REFRESH_TOKEN = os.environ.get("SELLSY_REFRESH_TOKEN", None)  # Optionnel

class ClientSynchronizer:
    """Classe pour synchroniser les clients entre Airtable et Sellsy."""
    
    def __init__(self):
        """Initialise les API clients."""
        # Vérification et initialisation des clés API
        if not all([
            Config.AIRTABLE_API_KEY, 
            Config.AIRTABLE_BASE_ID, 
            Config.AIRTABLE_TABLE_NAME,
            Config.SELLSY_CLIENT_ID,
            Config.SELLSY_CLIENT_SECRET
        ]):
            logger.error("❌ Paramètres de configuration manquants")
            raise ValueError("Configuration incomplète")
        
        self.airtable_api = AirtableAPI(
            Config.AIRTABLE_API_KEY,
            Config.AIRTABLE_BASE_ID,
            Config.AIRTABLE_TABLE_NAME
        )
        
        # Initialisation avec l'API Sellsy v2
        self.sellsy_api = SellsyAPI(
            Config.SELLSY_CLIENT_ID,
            Config.SELLSY_CLIENT_SECRET,
            Config.SELLSY_ACCESS_TOKEN,
            Config.SELLSY_REFRESH_TOKEN,
            logger
        )
        
        # Test de l'authentification Sellsy
        if not self.test_sellsy_connection():
            logger.error("❌ Échec de la connexion à l'API Sellsy")
            raise ConnectionError("L'authentification Sellsy a échoué")
        
        # Stockage temporaire du résultat de synchronisation
        self.sync_result = None
    
    def test_sellsy_connection(self) -> bool:
        """
        Teste la connexion à l'API Sellsy.
        
        Returns:
            True si la connexion réussit, False sinon
        """
        logger.info("🔄 Test de connexion à l'API Sellsy V2...")
        return self.sellsy_api.test_authentication()
    
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
        
        # Format pour l'API Sellsy V2 - par défaut on considère un individu (particulier)
        client_data = {
            "third": {
                "name": f"{prenom} {nom}",
                "email": email,
                "tel": telephone,
                "type": "person"  # Personne physique par défaut
            },
            "contact": {
                "name": nom,
                "firstname": prenom,
                "email": email,
                "tel": telephone,
                "position": "Client"
            },
            "address": {
                "name": "Adresse principale",
                "part1": adresse,
                "zip": code_postal,
                "town": ville,
                "countrycode": "FR"  # Par défaut France
            }
        }
        
        logger.info(f"✅ Données client validées pour {prenom} {nom}")
        return client_data

    def synchronize_client(self, record: Dict):
        """
        Synchronise un client d'Airtable vers Sellsy.
        
        Args:
            record: Enregistrement Airtable à synchroniser
        """
        record_fields = record.get('fields', {})
        record_id = record.get('id', 'inconnu')
        
        logger.info(f"🔄 Début de synchronisation pour l'enregistrement : {record_id}")
        
        # Réinitialiser le résultat de synchronisation
        self.sync_result = None
        
        # Préparation et validation des données
        formatted_data = self.sanitize_client_data(record_fields)
        
        if not formatted_data:
            logger.warning(f"⏩ Synchronisation ignorée pour {record_id} - données insuffisantes")
            return
        
        try:
            # Création du client dans Sellsy
            response = self.sellsy_api.create_client(formatted_data)

            if response:
                # Vérification de la réponse
                if response.get("status") == "success":
                    # Dans l'API Sellsy v2, l'ID client est dans le champ response
                    client_id = response.get("response")
                    
                    if client_id:
                        logger.info(f"✅ Client créé avec succès dans Sellsy. ID: {client_id}")
                        # Stocker le résultat pour le wrapper
                        self.sync_result = {"id": client_id}
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

def check_configuration() -> bool:
    """
    Vérifie que toute la configuration nécessaire est présente.
    
    Returns:
        True si la configuration est complète, False sinon
    """
    missing_configs = []
    
    if not Config.AIRTABLE_API_KEY:
        missing_configs.append("AIRTABLE_API_KEY")
    if not Config.AIRTABLE_BASE_ID:
        missing_configs.append("AIRTABLE_BASE_ID")
    if not Config.AIRTABLE_TABLE_NAME:
        missing_configs.append("AIRTABLE_TABLE_NAME")
    if not Config.SELLSY_CLIENT_ID:
        missing_configs.append("SELLSY_CLIENT_ID")
    if not Config.SELLSY_CLIENT_SECRET:
        missing_configs.append("SELLSY_CLIENT_SECRET")
        
    if missing_configs:
        logger.error(f"❌ Configuration incomplète. Variables manquantes: {', '.join(missing_configs)}")
        return False
        
    # Afficher les premières lettres des tokens pour le débogage (sans révéler les secrets)
    if Config.SELLSY_CLIENT_ID:
        logger.debug(f"SELLSY_CLIENT_ID: {Config.SELLSY_CLIENT_ID[:3]}...")
    if Config.SELLSY_CLIENT_SECRET:
        logger.debug(f"SELLSY_CLIENT_SECRET: {Config.SELLSY_CLIENT_SECRET[:3]}...")
    
    return True

def identify_sellsy_id_field(sample_records: List[Dict]) -> str:
    """
    Identifie le champ utilisé pour stocker l'ID Sellsy dans Airtable.
    
    Args:
        sample_records: Liste d'enregistrements Airtable pour analyse
    
    Returns:
        Nom du champ identifié ou 'ID_Sellsy' par défaut
    """
    possible_id_fields = [
        "ID_Sellsy", "Id_Sellsy", "id_sellsy", "ID Sellsy", "Id Sellsy", 
        "id sellsy", "IDSellsy", "Sellsy ID", "sellsy_id", "sellsy-id"
    ]
    
    if not sample_records or len(sample_records) == 0:
        logger.warning("⚠️ Aucun enregistrement disponible pour identification du champ ID Sellsy")
        return "ID_Sellsy"
    
    sample_fields = sample_records[0].get('fields', {})
    logger.debug(f"Champs disponibles dans Airtable: {list(sample_fields.keys())}")
    
    # Vérification des champs possibles
    for field in possible_id_fields:
        if field in sample_fields:
            logger.info(f"✓ Champ d'ID Sellsy identifié : {field}")
            return field
    
    logger.warning("⚠️ Impossible de déterminer le champ ID Sellsy. Utilisation par défaut: 'ID_Sellsy'")
    return "ID_Sellsy"

def main():
    """Fonction principale de synchronisation."""
    logger.info("🚀 Démarrage de la synchronisation des clients")
    start_time = time.time()
    
    try:
        # Vérification des configurations
        if not check_configuration():
            logger.error("❌ Configuration incomplète. Arrêt du processus.")
            return
        
        try:    
            # Initialisation du synchroniseur
            synchronizer = ClientSynchronizer()
            
            # Récupération d'un échantillon pour identifier le champ ID Sellsy
            sample_records = synchronizer.airtable_api.get_records(None, 1)
            sellsy_id_field = identify_sellsy_id_field(sample_records)
            
            # Récupération de tous les enregistrements
            logger.info(f"🔍 Récupération de tous les enregistrements pour filtrage local")
            all_records = synchronizer.airtable_api.get_records()
            logger.info(f"📊 Nombre total d'enregistrements dans Airtable: {len(all_records)}")
            
            # Filtrage des enregistrements sans ID Sellsy
            records_to_sync = []
            for record in all_records:
                fields = record.get('fields', {})
                
                # Vérifier si le champ existe et n'est pas vide
                if sellsy_id_field not in fields or not fields.get(sellsy_id_field):
                    records_to_sync.append(record)
                    continue
                
                # Vérifier si le champ contient une valeur vide, des espaces ou "None"
                id_value = str(fields.get(sellsy_id_field, "")).strip()
                if id_value == "" or id_value.lower() == "none":
                    records_to_sync.append(record)
                    # Log pour débug
                    logger.debug(f"Enregistrement sans ID valide trouvé: {record['id']} - Valeur: '{id_value}'")
            
            logger.info(f"📝 Nombre d'enregistrements à synchroniser: {len(records_to_sync)}")
            
            # Affichage des premiers enregistrements pour débogage
            if records_to_sync and len(records_to_sync) > 0:
                for idx, record in enumerate(records_to_sync[:3]):  # Afficher les 3 premiers pour le debug
                    logger.debug(f"Enregistrement #{idx+1} à synchroniser: {json.dumps({k: v for k, v in record.get('fields', {}).items() if k in ['Nom', 'Prenom', 'Email']})}")
                
                # Création d'un wrapper pour la synchronisation qui utilise le bon champ
                def sync_client_wrapper(record):
                    try:
                        # Synchronisation du client avec Sellsy
                        synchronizer.synchronize_client(record)
                        
                        # Si la synchronisation réussit, mettre à jour le champ ID Sellsy
                        if hasattr(synchronizer, 'sync_result') and synchronizer.sync_result:
                            client_id = synchronizer.sync_result.get('id')
                            if client_id:
                                update_result = synchronizer.airtable_api.update_record(
                                    record['id'], 
                                    {sellsy_id_field: str(client_id)}
                                )
                                
                                if update_result:
                                    logger.info(f"✅ ID Sellsy {client_id} mis à jour dans Airtable (champ: {sellsy_id_field})")
                                else:
                                    logger.error(f"❌ Échec de la mise à jour de l'ID Sellsy dans Airtable")
                    except Exception as e:
                        logger.error(f"❌ Erreur dans le wrapper de synchronisation: {str(e)}")
                
                # Si aucun enregistrement à synchroniser, terminer
                if not records_to_sync:
                    logger.info("⏹️ Aucun client sans ID Sellsy à synchroniser.")
                    return
                
                logger.info(f"🔄 Synchronisation de {len(records_to_sync)} clients")
                
                # Synchronisation de chaque client
                for i, record in enumerate(records_to_sync):
                    logger.info(f"Client {i+1}/{len(records_to_sync)}")
                    sync_client_wrapper(record)
                    # Pause légère entre les requêtes pour respecter les limites d'API
                    time.sleep(1)
            else:
                logger.info("⏹️ Aucun client à synchroniser")
                
        except (ValueError, ConnectionError) as e:
            logger.error(f"❌ Erreur critique: {str(e)}")
            return
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la récupération/traitement des enregistrements: {str(e)}")
            logger.exception("Détails de l'erreur:")
            return
        
        end_time = time.time()
        logger.info(f"✅ Synchronisation terminée en {end_time - start_time:.2f} secondes")
    
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'exécution globale : {e}")
        logger.exception("Détails complets de l'erreur:")

if __name__ == "__main__":
    main()
