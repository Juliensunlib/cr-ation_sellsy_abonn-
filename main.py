import os
import requests
from sellsy_api import sellsy_request

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = os.getenv("AIRTABLE_BASE_ID")
AIRTABLE_TABLE_NAME = os.getenv("AIRTABLE_TABLE_NAME")

AIRTABLE_URL = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
HEADERS = {
    "Authorization": f"Bearer {AIRTABLE_API_KEY}",
    "Content-Type": "application/json"
}


def get_airtable_records():
    print("[INFO] 📦 Récupération des enregistrements Airtable…")
    records = []
    offset = None
    while True:
        params = {}
        if offset:
            params['offset'] = offset
        resp = requests.get(AIRTABLE_URL, headers=HEADERS, params=params).json()
        page_records = resp.get("records", [])
        print(f"[INFO] ➕ {len(page_records)} enregistrements récupérés.")
        records.extend(page_records)
        offset = resp.get("offset")
        if not offset:
            break
    print(f"[INFO] ✅ Total des enregistrements : {len(records)}")
    return records


def sync_clients():
    print("[INFO] 🔄 Démarrage de la synchronisation des clients Sellsy...")
    records = get_airtable_records()
    for i, record in enumerate(records, start=1):
        fields = record.get("fields", {})
        client_id = fields.get("ID_Sellsy")
        nom = fields.get("Nom")
        prenom = fields.get("Prenom")

        if not nom or not prenom:
            print(f"[WARN] ⚠️ Ligne {i} ignorée : Nom ou prénom manquant.")
            continue

        print(f"[INFO] ▶️ Traitement du client : {nom} {prenom}")

        third = {
            "name": f"{nom} {prenom}",
            "email": fields.get("Email"),
            "tel": fields.get("Téléphone"),
            "type": "person"
        }

        contact = {
            "name": nom,
            "forename": prenom,
            "email": fields.get("Email"),
            "tel": fields.get("Téléphone")
        }

        address = {
            "name": f"{nom} {prenom}",
            "part1": fields.get("Adresse complète"),
            "zip": str(fields.get("Code postal")),
            "town": fields.get("Ville"),
            "countrycode": "FR"
        }

        try:
            if client_id:
                print(f"[INFO] ✏️ Mise à jour du client ID {client_id}")
                response = sellsy_request("Client.update", {
                    "clientid": client_id,
                    "third": third,
                    "contact": contact,
                    "address": address
                })
                print(f"[SUCCESS] ✅ Client {nom} mis à jour.")
            else:
                print(f"[INFO] 🆕 Création d’un nouveau client...")
                response = sellsy_request("Client.create", {
                    "third": third,
                    "contact": contact,
                    "address": address
                })
                new_id = response.get("response", {}).get("client_id")
                if new_id:
                    update_airtable_id(record["id"], new_id)
                    print(f"[SUCCESS] ✅ Client créé : {nom} (ID {new_id})")
                else:
                    print(f"[ERROR] ❌ Échec de création : {nom} - Réponse : {response}")
        except Exception as e:
            print(f"[ERROR] ❌ Erreur lors du traitement de {nom} {prenom} : {e}")


def update_airtable_id(record_id, client_id):
    url = f"{AIRTABLE_URL}/{record_id}"
    data = {
        "fields": {
            "ID_Sellsy": client_id
        }
    }
    resp = requests.patch(url, headers=HEADERS, json=data)
    if resp.status_code == 200:
        print(f"[INFO] 🔁 ID Sellsy {client_id} mis à jour dans Airtable.")
    else:
        print(f"[ERROR] ❌ Impossible de mettre à jour l’ID Airtable : {resp.text}")


if __name__ == "__main__":
    print("[START] 🚀 Script lancé depuis GitHub Actions.")
    sync_clients()
    print("[END] ✅ Fin de l’exécution.")
