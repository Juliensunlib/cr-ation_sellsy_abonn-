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
    records = []
    offset = None
    while True:
        params = {}
        if offset:
            params['offset'] = offset
        resp = requests.get(AIRTABLE_URL, headers=HEADERS, params=params).json()
        records.extend(resp.get("records", []))
        offset = resp.get("offset")
        if not offset:
            break
    return records


def sync_clients():
    records = get_airtable_records()
    for record in records:
        fields = record.get("fields", {})
        client_id = fields.get("ID_Sellsy")
        nom = fields.get("Nom")
        prenom = fields.get("Prenom")

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

        if client_id:
            # Mise à jour
            response = sellsy_request("Client.update", {
                "clientid": client_id,
                "third": third,
                "contact": contact,
                "address": address
