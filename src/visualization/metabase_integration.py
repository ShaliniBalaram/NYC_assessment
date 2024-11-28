"""Metabase integration for NYC TLC data visualization"""

import requests
import json
import logging
from typing import Dict, List, Optional
from visualization_config import METABASE_CONFIG, DASHBOARD_CONFIGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MetabaseIntegration:
    def __init__(self):
        self.base_url = f"http://{METABASE_CONFIG['host']}:{METABASE_CONFIG['port']}/api"
        self.session_token = None
        self._authenticate()

    def _authenticate(self):
        """Authenticate with Metabase"""
        try:
            auth_url = f"{self.base_url}/session"
            credentials = {
                "username": METABASE_CONFIG['username'],
                "password": METABASE_CONFIG['password']
            }
            response = requests.post(auth_url, json=credentials)
            response.raise_for_status()
            self.session_token = response.json()['id']
            logger.info("Successfully authenticated with Metabase")
        except Exception as e:
            logger.error(f"Failed to authenticate with Metabase: {str(e)}")
            raise

    def _get_headers(self) -> Dict:
        """Get headers with session token"""
        return {
            "X-Metabase-Session": self.session_token,
            "Content-Type": "application/json"
        }

    def create_database_connection(self) -> int:
        """Create BigQuery database connection in Metabase"""
        try:
            url = f"{self.base_url}/database"
            payload = {
                "name": METABASE_CONFIG['database_name'],
                "engine": "bigquery",
                "details": {
                    "project-id": METABASE_CONFIG['project_id'],
                    "dataset-id": METABASE_CONFIG['dataset_id'],
                    "service-account-json": METABASE_CONFIG['credentials_path']
                }
            }
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            db_id = response.json()['id']
            logger.info(f"Created database connection with ID: {db_id}")
            return db_id
        except Exception as e:
            logger.error(f"Failed to create database connection: {str(e)}")
            raise

    def create_card(self, db_id: int, card_config: Dict) -> int:
        """Create a new card (question/visualization) in Metabase"""
        try:
            url = f"{self.base_url}/card"
            payload = {
                "name": card_config['name'],
                "display": card_config['type'],
                "visualization_settings": {
                    "graph.dimensions": ["pickup_datetime"],
                    "graph.metrics": ["count"]
                },
                "dataset_query": {
                    "type": "native",
                    "native": {
                        "query": card_config['query']
                    },
                    "database": db_id
                }
            }
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            card_id = response.json()['id']
            logger.info(f"Created card '{card_config['name']}' with ID: {card_id}")
            return card_id
        except Exception as e:
            logger.error(f"Failed to create card: {str(e)}")
            raise

    def create_dashboard(self, name: str, card_ids: List[int]) -> int:
        """Create a new dashboard with the specified cards"""
        try:
            # Create dashboard
            url = f"{self.base_url}/dashboard"
            payload = {
                "name": name,
                "description": f"Dashboard for {name}"
            }
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            dashboard_id = response.json()['id']

            # Add cards to dashboard
            for i, card_id in enumerate(card_ids):
                add_card_url = f"{self.base_url}/dashboard/{dashboard_id}/cards"
                card_payload = {
                    "cardId": card_id,
                    "row": i * 4,  # Stagger cards vertically
                    "col": 0,
                    "sizeX": 6,
                    "sizeY": 4
                }
                response = requests.post(add_card_url, headers=self._get_headers(), json=card_payload)
                response.raise_for_status()

            logger.info(f"Created dashboard '{name}' with ID: {dashboard_id}")
            return dashboard_id
        except Exception as e:
            logger.error(f"Failed to create dashboard: {str(e)}")
            raise

    def setup_dashboards(self):
        """Set up all configured dashboards"""
        try:
            # Create database connection
            db_id = self.create_database_connection()

            # Create dashboards from config
            for dashboard_key, dashboard_config in DASHBOARD_CONFIGS.items():
                # Create cards for dashboard
                card_ids = []
                for card_config in dashboard_config['charts']:
                    card_id = self.create_card(db_id, card_config)
                    card_ids.append(card_id)

                # Create dashboard with cards
                dashboard_id = self.create_dashboard(dashboard_config['name'], card_ids)
                logger.info(f"Successfully set up dashboard '{dashboard_config['name']}'")

        except Exception as e:
            logger.error(f"Failed to set up dashboards: {str(e)}")
            raise

if __name__ == "__main__":
    metabase = MetabaseIntegration()
    metabase.setup_dashboards()
