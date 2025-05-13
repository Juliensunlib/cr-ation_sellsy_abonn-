import os
import requests
import logging
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
        self.logger = logging.getLogger('SellsySynchronizer')
    
    def get_records(self, filter_formula=None, limit=None) -> List[Dict]:
        """
        Récupère les enregistrements d'Airtable selon le filtre spécifié.
        
        Args:
            filter_formula: Formule de filtrage Airtable (ex: "BLANK({ID_Sellsy})")
            limit: Nombre maximum d'enregistrements à récupérer (optionnel)
            
        Returns:
            Liste des enregistrements
        """
        records = []
        offset = None
        
        self.logger.info(f"🔄 Récupération des enregistrements Airtable" + 
                        (f" avec filtre: {filter_formula}" if filter_formula else "") +
                        (f" (limité à {limit})" if limit else ""))
        
        try:
            while True:
                params = {}
                if offset:
                    params["offset"] = offset
                
                # Ajout du filtre si spécifié
                if filter_formula:
                    params["filterByFormula"] = filter_formula
                
                # Ajout de la limite si spécifiée
                if limit:
                    params["maxRecords"] = limit
                
                self.logger.debug(f"URL de requête: {self.base_url}")
                self.logger.debug(f"Paramètres: {params}")
                
                response = requests.get(
                    self.base_url, 
                    headers=self.headers, 
                    params=params
                )
                
                if response.status_code != 200:
                    self.logger.error(f"❌ Erreur API Airtable {response.status_code}: {response.text}")
                    return []
                
                data = response.json()
                
                page_records = data.get("records", [])
                self.logger.debug(f"Récupération de {len(page_records)} enregistrements dans cette page")
                records.extend(page_records)
                
                # Si une limite est définie et atteinte, arrêtons-nous
                if limit and len(records) >= limit:
                    records = records[:limit]  # Assurons-nous de ne pas dépasser la limite
                    break
                
                offset = data.get("offset")
                if not offset:
                    break
                
            self.logger.info(f"✅ {len(records)} enregistrements récupérés au total")
            return records
            
        except requests.RequestException as e:
            self.logger.error(f"❌ Erreur lors de la requête Airtable: {str(e)}")
            if hasattr(e, 'response') and e.response:
                self.logger.error(f"Status code: {e.response.status_code}")
                self.logger.error(f"Détails: {e.response.text}")
            return []
        except Exception as e:
            self.logger.error(f"❌ Erreur inattendue lors de la récupération des enregistrements: {str(e)}")
            return []
    
    def update_record(self, record_id: str, fields: Dict) -> Dict:
        """
        Met à jour un enregistrement dans Airtable.
        
        Args:
            record_id: ID de l'enregistrement à mettre à jour
            fields: Dictionnaire des champs à mettre à jour
            
        Returns:
            Réponse de l'API Airtable
        """
        try:
            self.logger.debug(f"Mise à jour de l'enregistrement {record_id} avec les champs: {fields}")
            
            response = requests.patch(
                f"{self.base_url}/{record_id}", 
                headers=self.headers, 
                json={"fields": fields}
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Erreur lors de la mise à jour {response.status_code}: {response.text}")
                return None
            
            self.logger.info(f"✅ Enregistrement {record_id} mis à jour avec succès")
            return response.json()
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la mise à jour de l'enregistrement: {str(e)}")
            return None
    
    def create_record(self, fields: Dict) -> Dict:
        """
        Crée un nouvel enregistrement dans Airtable.
        
        Args:
            fields: Dictionnaire des champs pour le nouvel enregistrement
            
        Returns:
            Réponse de l'API Airtable
        """
        try:
            self.logger.debug(f"Création d'un nouvel enregistrement avec les champs: {fields}")
            
            response = requests.post(
                self.base_url, 
                headers=self.headers, 
                json={"fields": fields}
            )
            
            if response.status_code != 200:
                self.logger.error(f"❌ Erreur lors de la création {response.status_code}: {response.text}")
                return None
                
            self.logger.info(f"✅ Nouvel enregistrement créé avec succès")
            return response.json()
            
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de la création de l'enregistrement: {str(e)}")
            return None
