from sellsy_api import sellsy_request
from airtable_api import get_airtable_records, update_airtable_record

def sync_clients():
    records = get_airtable_records()
    for record in records:
        fields = record["fields"]
        record_id = record["id"]

        if "ID_Sellsy" not in fields:
            params = {
                "third": {
                    "name": fields.get("Nom", ""),
                    "type": "person",
                    "email": fields.get("Email", ""),
                    "tel": fields.get("Téléphone", "")
                },
                "contact": {
                    "name": fields.get("Nom", ""),
                    "forename": fields.get("Prenom", ""),
                    "email": fields.get("Email", ""),
                    "tel": fields.get("Téléphone", "")
                },
                "address": {
                    "name": fields.get("Nom", ""),
                    "part1": fields.get("Adresse complète", ""),
                    "zip": str(fields.get("Code postal", "")),
                    "town": fields.get("Ville", ""),
                    "countrycode": "FR"
                }
            }

            result = sellsy_request("Client.create", params)
            sellsy_id = result.get("response", {}).get("client_id")

            if sellsy_id:
                update_airtable_record(record_id, {"ID_Sellsy": sellsy_id})

if __name__ == "__main__":
    sync_clients()
