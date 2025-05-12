import requests
import os
from dotenv import load_dotenv

load_dotenv()

AIRTABLE_URL = f"https://api.airtable.com/v0/{os.getenv('AIRTABLE_BASE_ID')}/{os.getenv('AIRTABLE_TABLE_NAME')}"

def get_airtable_records():
    headers = {"Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}"}
    response = requests.get(AIRTABLE_URL, headers=headers)
    return response.json()["records"]

def update_airtable_record(record_id, fields):
    headers = {
        "Authorization": f"Bearer {os.getenv('AIRTABLE_API_KEY')}",
        "Content-Type": "application/json"
    }
    data = {"fields": fields}
    requests.patch(f"{AIRTABLE_URL}/{record_id}", headers=headers, json=data)
