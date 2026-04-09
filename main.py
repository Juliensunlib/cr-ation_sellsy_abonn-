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
        # Vérifier d'abord si c'est une entreprise ou un particulier
        nom_entreprise = str(record_fields.get("Nom de l'entreprise", "")).strip()
        
        if nom_entreprise:
            # Pour les entreprises : pas besoin de Nom/Prénom individuels
            required_fields = [
                'Nom de l\'entreprise', 'Email', 
                'Adresse complète', 'Code postal', 'Ville'
            ]
        else:
            # Pour les particuliers : Nom et Prénom sont requis
            required_fields = [
                'Nom', 'Prenom', 'Email', 
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
        
        # Nettoyage des données communes
        email = str(record_fields["Email"]).strip()
        telephone = str(record_fields.get("Téléphone", "")).strip() if record_fields.get("Téléphone") else ""
        adresse = str(record_fields["Adresse complète"]).strip()
        code_postal = str(record_fields["Code postal"]).strip()
        ville = str(record_fields["Ville"]).strip()
        
        # Récupération du champ pays s'il existe, sinon "FR" par défaut
        pays_code = str(record_fields.get("Pays", "FR")).strip()
        if pays_code == "":
            pays_code = "FR"
        
        # Récupération de l'adresse ligne 2 si elle existe
        adresse_ligne_2 = str(record_fields.get("Adresse ligne 2", "")).strip()
        
        # Récupération du champ installateur (peut être une liste dans Airtable)
        installateur_raw = record_fields.get("Nom de l'entreprise (from Installateur)", "")
        if isinstance(installateur_raw, list) and len(installateur_raw) > 0:
            installateur = str(installateur_raw[0]).strip()
        elif installateur_raw:
            installateur = str(installateur_raw).strip()
        else:
            installateur = ""

        # Récupération du champ puissance installée en kWc
        puissance_kwc_raw = record_fields.get("Puissance installée en kWc", "")
        if isinstance(puissance_kwc_raw, (int, float)):
            puissance_kwc = str(puissance_kwc_raw)
        elif puissance_kwc_raw:
            puissance_kwc = str(puissance_kwc_raw).strip()
        else:
            puissance_kwc = ""

        # Récupération du champ contrat abonné pour la référence
        contrat_abonne = str(record_fields.get("Contrat abonné", "")).strip()
        
        # Vérification du format de l'email
        if "@" not in email:
            logger.warning(f"⚠️ Format d'email invalide: {email}")
            return None
        
        # Format pour l'API Sellsy V2 selon le type de client
        if nom_entreprise:
            # C'est une entreprise - pas de contact individuel
            client_data = {
                "third": {
                    "name": nom_entreprise,
                    "email": email,
                    "type": "corporation"
                },
                "contact": {
                    "name": nom_entreprise,
                    "firstname": "",
                    "email": email,
                    "position": "Entreprise"
                },
                "address": {
                    "name": "Adresse principale",
                    "address_line_1": adresse,
                    "address_line_2": adresse_ligne_2,
                    "postal_code": code_postal,
                    "city": ville,
                    "country": {
                        "code": pays_code
                    },
                    "is_invoicing_address": True,
                    "is_delivery_address": True,
                    "is_main": True
                }
            }
            
            # Ajouter le téléphone seulement s'il est présent
            if telephone:
                client_data["third"]["tel"] = telephone
                client_data["contact"]["tel"] = telephone
            
            siret = str(record_fields.get("SIRET", "")).strip()
            if siret:
                client_data["third"]["siret"] = siret
            
            # Ajout du champ installateur si présent
            if installateur:
                client_data["third"]["installateur"] = installateur

            # Ajout du champ puissance en kWc si présent
            if puissance_kwc:
                client_data["third"]["puissance_en_kwc"] = puissance_kwc

            # Ajout de la référence si présente
            if contrat_abonne:
                client_data["third"]["contrat_abonne"] = contrat_abonne
                
            logger.info(f"✅ Données entreprise validées pour {nom_entreprise}")
        else:
            # C'est un particulier
            nom = str(record_fields["Nom"]).strip()
            prenom = str(record_fields["Prenom"]).strip()
            
            client_data = {
                "third": {
                    "name": f"{prenom} {nom}",
                    "email": email,
                    "type": "person"
                },
                "contact": {
                    "name": nom,
                    "firstname": prenom,
                    "email": email,
                    "position": "Client"
                },
                "address": {
                    "name": "Adresse principale",
                    "address_line_1": adresse,
                    "address_line_2": adresse_ligne_2,
                    "postal_code": code_postal,
                    "city": ville,
                    "country": {
                        "code": pays_code
                    },
                    "is_invoicing_address": True,
                    "is_delivery_address": True,
                    "is_main": True
                }
            }
            
            # Ajouter le téléphone seulement s'il est présent
            if telephone:
                client_data["third"]["tel"] = telephone
                client_data["contact"]["tel"] = telephone
            
            # Ajout du champ installateur si présent
            if installateur:
                client_data["third"]["installateur"] = installateur

            # Ajout du champ puissance en kWc si présent
            if puissance_kwc:
                client_data["third"]["puissance_en_kwc"] = puissance_kwc

            # Ajout de la référence si présente
            if contrat_abonne:
                client_data["third"]["contrat_abonne"] = contrat_abonne
            
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

        # Vérification du champ "Contrat d'abonnement signé"
        contrat_signe = record_fields.get("Contrat d'abonnement signé", False)
        if not contrat_signe:
            logger.info(f"⏩ Synchronisation ignorée pour {record_id} - Contrat d'abonnement non signé")
            return

        # Préparation et validation des données
        formatted_data = self.sanitize_client_data(record_fields)
        
        if not formatted_data:
            logger.warning(f"⏩ Synchronisation ignorée pour {record_id} - données insuffisantes")
            return
        
        try:
            # Déterminer si le client est un individu ou une entreprise
            is_individual = formatted_data["third"]["type"] == "person"
            
            # Extraire les données d'adresse avant la création du client
            address_data = formatted_data.get("address", {})
            
            # Création du client dans Sellsy
            response = self.sellsy_api.create_client(formatted_data)

            if response and response.get("status") == "success":
                # Dans l'API Sellsy v2, l'ID client est dans le champ response
                client_id = response.get("client_id") or response.get("response")
                
                if client_id:
                    logger.info(f"✅ Client créé avec succès dans Sellsy. ID: {client_id}")
                    
                    # Créer explicitement l'adresse pour tous les clients, qu'ils soient individus ou entreprises
                    # (Cela résout le problème des adresses non synchronisées)
                    address_result = self.create_address_for_client(client_id, address_data, is_individual)
                    if address_result:
                        logger.info(f"✅ Adresse créée avec succès pour le client ID: {client_id}")
                    else:
                        logger.warning(f"⚠️ Échec de création d'adresse pour le client ID: {client_id}")
                    
                    # Stocker le résultat pour le wrapper
                    # Assurons-nous de stocker uniquement l'ID comme chaîne
                    self.sync_result = {"id": str(client_id)}
                else:
                    logger.error(f"❌ Impossible de trouver l'ID client dans la réponse: {response}")
            else:
                error_msg = response.get("error", "Réponse inconnue") if response else "Pas de réponse"
                logger.error(f"🚨 Échec de la synchronisation du client: {error_msg}")
        
        except Exception as e:
            logger.error(f"❌ Erreur lors de la synchronisation : {str(e)}")
            logger.exception("Détails de l'erreur:")
    
    def create_address_for_client(self, client_id: str, address_data: Dict, is_individual: bool) -> bool:
        """
        Crée une adresse pour un client dans Sellsy.
        
        Args:
            client_id: ID du client dans Sellsy
            address_data: Données de l'adresse
            is_individual: True si le client est un particulier, False sinon
            
        Returns:
            True si la création a réussi, False sinon
        """
        try:
            # Formater l'adresse pour l'API Sellsy v2
            formatted_address = {
                "name": address_data.get("name", "Adresse principale"),
                "address_line_1": address_data.get("address_line_1", ""),
                "address_line_2": address_data.get("address_line_2", ""),
                "postal_code": address_data.get("postal_code", ""),
                "city": address_data.get("city", ""),
                "country_code": address_data.get("country", {}).get("code", "FR") if isinstance(address_data.get("country", {}), dict) else address_data.get("country", "FR")
            }
            
            logger.debug(f"Création d'adresse pour client {client_id}: {formatted_address}")
            
            # Créer l'adresse via l'API Sellsy
            result = self.sellsy_api.create_address(client_id, formatted_address, is_individual)
            
            return result is not None
            
        except Exception as e:
            logger.error(f"❌ Erreur lors de la création de l'adresse: {str(e)}")
            return False

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
                            # Extraction de l'ID client comme chaîne simple
                            client_id = str(synchronizer.sync_result.get('id'))
                            if client_id:
                                # Mise à jour du champ ID Sellsy dans Airtable avec une simple chaîne
                                update_result = synchronizer.airtable_api.update_record(
                                    record['id'], 
                                    {sellsy_id_field: client_id}
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
