import os
import logging
from dotenv import load_dotenv
import requests
from pyairtable import Api as AirtableApi

# Charger les variables d'environnement
load_dotenv()

# Configurer la journalisation
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration Airtable
AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
AIRTABLE_TABLE_NAME = os.getenv('AIRTABLE_TABLE_NAME')

# Configuration Sellsy
SELLSY_CONSUMER_TOKEN = os.getenv('SELLSY_CONSUMER_TOKEN')
SELLSY_CONSUMER_SECRET = os.getenv('SELLSY_CONSUMER_SECRET')
SELLSY_USER_TOKEN = os.getenv('SELLSY_USER_TOKEN')
SELLSY_USER_SECRET = os.getenv('SELLSY_USER_SECRET')

class ClientSellsy:
    URL_DE_BASE = 'https://api.sellsy.com/api/v1'

    @staticmethod
    def _creer_requete_sellsy(methode, parametres):
        """Créer une requête pour l'API Sellsy."""
        return {
            'method': methode,
            'params': parametres
        }

    def creer_client(self, donnees_client):
        """Créer un client dans Sellsy."""
        requete_payload = self._creer_requete_sellsy('Client.create', {
            'third': {
                'name': donnees_client.get('Nom', ''),
                'email': donnees_client.get('Email', ''),
                'tel': donnees_client.get('Téléphone', ''),
                'type': 'person'  # Clients individuels
            },
            'contact': {
                'name': donnees_client.get('Nom', ''),
                'forename': donnees_client.get('Prenom', ''),
                'email': donnees_client.get('Email', ''),
                'tel': donnees_client.get('Téléphone', '')
            },
            'address': {
                'name': donnees_client.get('Nom', ''),
                'part1': donnees_client.get('Adresse complète', ''),
                'zip': donnees_client.get('Code postal', ''),
                'town': donnees_client.get('Ville', ''),
                'countrycode': 'FR'  # Défaut sur la France
            }
        })

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {SELLSY_USER_TOKEN}'
        }

        try:
            reponse = requests.post(
                f'{self.URL_DE_BASE}/methods',
                json=requete_payload,
                headers=headers
            )
            reponse.raise_for_status()
            return reponse.json()
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la création du client Sellsy : {e}")
            return None

    def mettre_a_jour_client(self, id_client, donnees_client):
        """Mettre à jour un client existant dans Sellsy."""
        requete_payload = self._creer_requete_sellsy('Client.update', {
            'clientid': id_client,
            'third': {
                'name': donnees_client.get('Nom', ''),
                'email': donnees_client.get('Email', ''),
                'tel': donnees_client.get('Téléphone', ''),
                'type': 'person'
            },
            'contact': {
                'name': donnees_client.get('Nom', ''),
                'forename': donnees_client.get('Prenom', ''),
                'email': donnees_client.get('Email', ''),
                'tel': donnees_client.get('Téléphone', '')
            },
            'address': {
                'name': donnees_client.get('Nom', ''),
                'part1': donnees_client.get('Adresse complète', ''),
                'zip': donnees_client.get('Code postal', ''),
                'town': donnees_client.get('Ville', ''),
                'countrycode': 'FR'
            }
        })

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {SELLSY_USER_TOKEN}'
        }

        try:
            reponse = requests.post(
                f'{self.URL_DE_BASE}/methods',
                json=requete_payload,
                headers=headers
            )
            reponse.raise_for_status()
            return reponse.json()
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la mise à jour du client Sellsy : {e}")
            return None

    def supprimer_client(self, id_client):
        """Supprimer un client de Sellsy."""
        requete_payload = self._creer_requete_sellsy('Client.delete', {
            'clientid': id_client
        })

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {SELLSY_USER_TOKEN}'
        }

        try:
            reponse = requests.post(
                f'{self.URL_DE_BASE}/methods',
                json=requete_payload,
                headers=headers
            )
            reponse.raise_for_status()
            return reponse.json()
        except requests.RequestException as e:
            logger.error(f"Erreur lors de la suppression du client Sellsy : {e}")
            return None

def synchroniser_airtable_vers_sellsy():
    """Synchroniser les enregistrements Airtable avec Sellsy."""
    # Initialiser le client Airtable
    client_airtable = AirtableApi(AIRTABLE_API_KEY)
    table = client_airtable.table(AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME)

    # Initialiser le client Sellsy
    client_sellsy = ClientSellsy()

    # Récupérer tous les enregistrements Airtable
    enregistrements = table.all()

    for enregistrement in enregistrements:
        champs = enregistrement['fields']
        
        # Vérifier si l'enregistrement a déjà un ID Sellsy
        id_sellsy = champs.get('ID_Sellsy')

        try:
            # Si aucun ID Sellsy n'existe, créer un nouveau client
            if not id_sellsy:
                reponse = client_sellsy.creer_client(champs)
                if reponse and reponse.get('status') == 'success':
                    # Mettre à jour Airtable avec l'ID du client Sellsy
                    nouvel_id_client = reponse['response'].get('client_id')
                    table.update(enregistrement['id'], {'ID_Sellsy': nouvel_id_client})
                    logger.info(f"Client Sellsy créé pour {champs.get('Nom', 'Inconnu')}")
            else:
                # Mettre à jour le client existant
                reponse = client_sellsy.mettre_a_jour_client(int(id_sellsy), champs)
                if reponse and reponse.get('status') == 'success':
                    logger.info(f"Client Sellsy {id_sellsy} mis à jour pour {champs.get('Nom', 'Inconnu')}")

        except Exception as e:
            logger.error(f"Erreur lors du traitement de l'enregistrement {enregistrement['id']} : {e}")

def principale():
    synchroniser_airtable_vers_sellsy()

if __name__ == '__main__':
    principale()
