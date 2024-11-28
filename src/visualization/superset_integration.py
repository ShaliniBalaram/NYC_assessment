"""Apache Superset integration for NYC TLC data visualization"""

import requests
import json
import logging
from typing import Dict, List, Optional
from visualization_config import SUPERSET_CONFIG, DASHBOARD_CONFIGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupersetIntegration:
    def __init__(self):
        self.base_url = f"http://{SUPERSET_CONFIG['host']}:{SUPERSET_CONFIG['port']}/api/v1"
        self.access_token = None
        self._authenticate()

    def _authenticate(self):
        """Authenticate with Superset"""
        try:
            auth_url = f"{self.base_url}/security/login"
            credentials = {
                "username": SUPERSET_CONFIG['username'],
                "password": SUPERSET_CONFIG['password'],
                "provider": "db"
            }
            response = requests.post(auth_url, json=credentials)
            response.raise_for_status()
            self.access_token = response.json()['access_token']
            logger.info("Successfully authenticated with Superset")
        except Exception as e:
            logger.error(f"Failed to authenticate with Superset: {str(e)}")
            raise

    def _get_headers(self) -> Dict:
        """Get headers with authentication token"""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }

    def create_database_connection(self) -> int:
        """Create BigQuery database connection in Superset"""
        try:
            url = f"{self.base_url}/database/"
            payload = {
                "database_name": SUPERSET_CONFIG['database_name'],
                "sqlalchemy_uri": SUPERSET_CONFIG['sqlalchemy_uri'],
                "expose_in_sqllab": True,
                "allow_ctas": True,
                "allow_cvas": True
            }
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            db_id = response.json()['id']
            logger.info(f"Created database connection with ID: {db_id}")
            return db_id
        except Exception as e:
            logger.error(f"Failed to create database connection: {str(e)}")
            raise

    def create_chart(self, db_id: int, chart_config: Dict) -> int:
        """Create a new chart in Superset"""
        try:
            url = f"{self.base_url}/chart/"
            payload = {
                "datasource_id": db_id,
                "datasource_type": "table",
                "viz_type": chart_config['type'],
                "params": {
                    "viz_type": chart_config['type'],
                    "granularity_sqla": "pickup_datetime",
                    "time_grain_sqla": "P1D",
                    "metrics": ["count"],
                    "adhoc_filters": [],
                    "groupby": [],
                    "row_limit": 10000,
                    "query_mode": "raw",
                    "custom_sql": chart_config['query']
                },
                "query_context": json.dumps({
                    "datasource": {"id": db_id, "type": "table"},
                    "force": False,
                    "queries": [{
                        "time_range": "No filter",
                        "granularity": "pickup_datetime",
                        "filters": [],
                        "extras": {"time_grain_sqla": "P1D"},
                        "applied_time_extras": {},
                        "columns": [],
                        "metrics": ["count"],
                        "annotation_layers": [],
                        "row_limit": 10000,
                        "custom_sql": chart_config['query']
                    }]
                })
            }
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            chart_id = response.json()['id']
            logger.info(f"Created chart '{chart_config['name']}' with ID: {chart_id}")
            return chart_id
        except Exception as e:
            logger.error(f"Failed to create chart: {str(e)}")
            raise

    def create_dashboard(self, name: str, chart_ids: List[int]) -> int:
        """Create a new dashboard with the specified charts"""
        try:
            url = f"{self.base_url}/dashboard/"
            payload = {
                "dashboard_title": name,
                "slug": name.lower().replace(" ", "_"),
                "position_json": json.dumps({
                    "DASHBOARD_VERSION_KEY": "v2",
                    "ROOT_ID": {"children": [
                        {"id": f"CHART-{chart_id}", "meta": {"width": 6, "height": 50}}
                        for chart_id in chart_ids
                    ]}
                })
            }
            response = requests.post(url, headers=self._get_headers(), json=payload)
            response.raise_for_status()
            dashboard_id = response.json()['id']
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
                # Create charts for dashboard
                chart_ids = []
                for chart_config in dashboard_config['charts']:
                    chart_id = self.create_chart(db_id, chart_config)
                    chart_ids.append(chart_id)

                # Create dashboard with charts
                dashboard_id = self.create_dashboard(dashboard_config['name'], chart_ids)
                logger.info(f"Successfully set up dashboard '{dashboard_config['name']}'")

        except Exception as e:
            logger.error(f"Failed to set up dashboards: {str(e)}")
            raise

if __name__ == "__main__":
    superset = SupersetIntegration()
    superset.setup_dashboards()
