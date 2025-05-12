import os
import requests
import time

# Chargement des variables d'environnement depuis GitHub secrets
AIRTABLE_API_KEY = os.environ.get("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.environ.get("AIRTABLE_TABLE_NAME")

SELLSY_API_CONSUMER_TOKEN = os.environ.get("SELLSY_API_CONSUMER_TOKEN")
SELLSY_API_CONSUMER_SECRET = os.environ.get("SELLSY_API_CONSUMER_SECRET")
SELLSY_API_USER_TOKEN = os.environ.get("SELLSY_API_USER_TOKEN")
SELLSY_API_USER_SECRET = os.environ.get("SELLSY_API_USER_SECRET")

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}

# Fonction pour récupérer les enregistrements depuis Airtable
def get_airtable_records():
    print("[INFO] 📦 Récupération des enregistrements Airtable…")
    records = []
    offset = None
    while True:
        params = {}
        if offset:
            params['offset'] = offset

        print(f"[DEBUG] 🔍 Requête envoyée à Airtable : {AIRTABLE_URL}")
        if offset:
            print(f"[DEBUG] 🔁 Avec offset : {offset}")

        try:
            response = requests.get(AIRTABLE_URL, headers=HEADERS, params=params)
            print(f"[DEBUG] 📩 Code réponse : {response.status_code}")
            if response.status_code != 200:
                print(f"[ERROR] ❌ Erreur d’appel API Airtable : {response.text}")
                break

            resp_json = response.json()
            print(f"[DEBUG] 📄 Réponse JSON (tronquée) : {str(resp_json)[:500]}...")  # tronque pour éviter log trop long

            page_records = resp_json.get("records", [])
            print(f"[INFO] ➕ {len(page_records)} enregistrements récupérés dans cette page.")

            records.extend(page_records)

            offset = resp_json.get("offset")
            if not offset:
                break

        except Exception as e:
            print(f"[ERROR] 🚨 Exception lors de l’appel Airtable : {e}")
            break

    print(f"[INFO] ✅ Total des enregistrements récupérés : {len(records)}")
    if len(records) == 0:
        print("[WARN] ⚠️ Aucun enregistrement trouvé. Vérifie les filtres, permissions ou contenus d’Airtable.")
    return records

# Fonction pour envoyer une requête à l'API Sellsy
def sellsy_request(method, params):
    print(f"[INFO] 📝 Envoi de la requête à l’API Sellsy : {method}")
    url = "https://api.sellsy.com/0/"
    headers = {
        "Content-Type": "application/json"
    }
    request_data = {
        "method": method,
        "params": params
    }
    
    try:
        response = requests.post(url, json=request_data, auth=(SELLSY_API_CONSUMER_TOKEN, SELLSY_API_USER_TOKEN), headers=headers)
        print(f"[DEBUG] 📩 Code réponse : {response.status_code}")
        if response.status_code != 200:
            print(f"[ERROR] ❌ Erreur d’appel API Sellsy : {response.text}")
            return None
        
        response_json = response.json()
        print(f"[DEBUG] 📄 Réponse JSON (tronquée) : {str(response_json)[:500]}...")  # tronque pour éviter log trop long
        return response_json

    except Exception as e:
        print(f"[ERROR] 🚨 Exception lors de l’appel API Sellsy : {e}")
        return None

# Fonction principale pour synchroniser les clients
def synchronize_clients():
    print("[START] 🚀 Script lancé depuis GitHub Actions.")
    records = get_airtable_records()

    if records:
        print(f"[INFO] 🔄 Démarrage de la synchronisation des clients Sellsy...")
        for record in records:
            fields = record.get('fields', {})
            if not all(k in fields for k in ['Nom', 'Prenom', 'Email', 'Téléphone', 'Adresse complète', 'Code postal', 'Ville']):
                print(f"[WARN] ⚠️ Enregistrement incomplet pour {record['id']}.")
                continue

            client_data = {
                "name": fields["Nom"],
                "forename": fields["Prenom"],
                "email": fields["Email"],
                "tel": fields["Téléphone"],
                "address": fields["Adresse complète"],
                "zip": fields["Code postal"],
                "town": fields["Ville"]
            }

            print(f"[INFO] ➕ Synchronisation du client : {client_data['name']} {client_data['forename']}")

            # Appel à l'API Sellsy pour créer le client
            response = sellsy_request("Client.create", {"third": client_data})

            if response:
                client_id = response.get('response', {}).get('client_id')
                if client_id:
                    print(f"[INFO] ✅ Client créé avec succès. ID Sellsy : {client_id}")
                    # Mettre à jour l'ID_Sellsy dans Airtable
                    # Mettre à jour l'enregistrement avec le client_id
                    # Utiliser l'API Airtable pour mettre à jour ce champ si nécessaire
                else:
                    print(f"[ERROR] ❌ Le client n'a pas été créé dans Sellsy.")
            else:
                print("[ERROR] 🚨 Échec de la synchronisation du client.")
    else:
        print("[INFO] Aucun client à synchroniser.")

    print("[END] ✅ Fin de l’exécution.")

# Exécution toutes les 3 heures
if __name__ == "__main__":
    while True:
        synchronize_clients()
        print("[INFO] ⏱️ Pause de 3 heures avant la prochaine synchronisation...")
        time.sleep(10800)  # 10800 secondes = 3 heures
