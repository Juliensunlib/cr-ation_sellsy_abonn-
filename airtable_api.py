import os
import requests
from typing import Dict, List, Optional

class AirtableAPI:
    def __init__(self, api_key, base_id, table_name):
        self.api_key = api_key
        self.base_id = base_id
        self.table_name = table_name
        self.base_url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def get_records(self, filter_formula=None) -> List[Dict]:
        """
        Récupère les enregistrements d'Airtable selon le filtre spécifié.
        
        Args:
            filter_formula: Formule de filtrage Airtable (ex: "BLANK({ID_Sellsy})")
            
        Returns:
            Liste des enregistrements
        """
        records = []
        offset = None
        
        while True:
            params = {}
            if offset:
                params["offset"] = offset
            
            # Ajout du filtre si spécifié
            if filter_formula:
                params["filterByFormula"] = filter_formula
            
            response = requests.get(
                self.base_url, 
                headers=self.headers, 
                params=params
            )
            
            response.raise_for_status()
            data = response.json()
            
            page_records = data.get("records", [])
            records.extend(page_records)
            
            offset = data.get("offset")
            if not offset:
                break
                
        return records
    
    def update_record(self, record_id: str, fields: Dict) -> Dict:
        """
        Met à jour un enregistrement dans Airtable.
        
        Args:
            record_id: ID de l'enregistrement à mettre à jour
            fields: Dictionnaire des champs à mettre à jour
            
        Returns:
            Réponse de l'API Airtable
        """
        response = requests.patch(
            f"{self.base_url}/{record_id}", 
            headers=self.headers, 
            json={"fields": fields}
        )
        
        response.raise_for_status()
        return response.json()
    
    def create_record(self, fields: Dict) -> Dict:
        """
        Crée un nouvel enregistrement dans Airtable.
        
        Args:
            fields: Dictionnaire des champs pour le nouvel enregistrement
            
        Returns:
            Réponse de l'API Airtable
        """
        response = requests.post(
            self.base_url, 
            headers=self.headers, 
            json={"fields": fields}
        )
        
        response.raise_for_status()
        return response.json()
