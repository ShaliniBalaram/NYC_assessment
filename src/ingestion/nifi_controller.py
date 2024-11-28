import requests
from requests.auth import HTTPBasicAuth
import json
import time
import urllib3
import logging

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NiFiController:
    def __init__(self):
        self.base_url = 'https://localhost:9090/nifi-api'
        self.username = 'c54524d4-5f5a-4b82-a573-b091ca1a6722'
        self.password = 'zFoQadWKtQwacSvji7jgtYfBMLO2XXNk'
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Requested-With': 'XMLHttpRequest'
        }
        self.access_token = None
        self._authenticate()

    def _authenticate(self):
        """Get access token for NiFi API"""
        try:
            # First try to get access token
            response = requests.post(
                f"{self.base_url}/access/token",
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': '*/*'
                },
                data={
                    'username': self.username,
                    'password': self.password
                },
                verify=False
            )
            response.raise_for_status()
            self.access_token = response.text
            self.headers['Authorization'] = f'Bearer {self.access_token}'
            logging.info("Successfully authenticated with NiFi")
        except Exception as e:
            logging.error(f"Error authenticating with NiFi: {str(e)}")
            if response is not None:
                logging.error(f"Response content: {response.text}")
            raise

    def get_root_pg_id(self):
        """Get the root process group ID"""
        try:
            response = requests.get(
                f"{self.base_url}/flow/process-groups/root",
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()  # Raise an error for bad status codes
            return response.json()['processGroupFlow']['id']
        except Exception as e:
            logging.error(f"Error getting root process group ID: {str(e)}")
            if response is not None:
                logging.error(f"Response content: {response.text}")
            raise

    def create_process_group(self, name, position):
        """Create a new process group"""
        try:
            root_pg_id = self.get_root_pg_id()
            payload = {
                "revision": {
                    "clientId": "",
                    "version": 0
                },
                "disconnectedNodeAcknowledged": False,
                "component": {
                    "name": name,
                    "position": position
                }
            }
            response = requests.post(
                f"{self.base_url}/process-groups/{root_pg_id}/process-groups",
                json=payload,
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error creating process group: {str(e)}")
            if response is not None:
                logging.error(f"Response content: {response.text}")
            raise

    def create_processor(self, group_id, processor_type, position, name):
        """Create a new processor"""
        try:
            payload = {
                "revision": {
                    "clientId": "",
                    "version": 0
                },
                "disconnectedNodeAcknowledged": False,
                "component": {
                    "type": processor_type,
                    "name": name,
                    "position": position,
                    "config": {
                        "concurrentlySchedulableTaskCount": "1",
                        "schedulingPeriod": "0 sec"
                    }
                }
            }
            response = requests.post(
                f"{self.base_url}/process-groups/{group_id}/processors",
                json=payload,
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error creating processor: {str(e)}")
            if response is not None:
                logging.error(f"Response content: {response.text}")
            raise

    def update_processor(self, processor_id, properties, scheduling):
        """Update processor properties and scheduling"""
        try:
            # First get current processor info
            response = requests.get(
                f"{self.base_url}/processors/{processor_id}",
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()
            processor_data = response.json()

            # Update with new properties
            processor_data['component']['properties'] = properties  # Replace all properties
            processor_data['component']['config']['schedulingPeriod'] = scheduling
            processor_data['revision']['clientId'] = ""
            processor_data['component']['config']['autoTerminatedRelationships'] = [
                "Failure", "No Retry", "Retry"  # Auto-terminate these relationships
            ]
            processor_data['disconnectedNodeAcknowledged'] = False

            response = requests.put(
                f"{self.base_url}/processors/{processor_id}",
                json=processor_data,
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error updating processor: {str(e)}")
            if response is not None:
                logging.error(f"Response content: {response.text}")
            raise

    def create_connection(self, source_id, destination_id, group_id):
        """Create a connection between processors"""
        try:
            payload = {
                "revision": {
                    "clientId": "",
                    "version": 0
                },
                "disconnectedNodeAcknowledged": False,
                "component": {
                    "source": {
                        "id": source_id,
                        "groupId": group_id,
                        "type": "PROCESSOR"
                    },
                    "destination": {
                        "id": destination_id,
                        "groupId": group_id,
                        "type": "PROCESSOR"
                    },
                    "selectedRelationships": ["success"]
                }
            }
            response = requests.post(
                f"{self.base_url}/process-groups/{group_id}/connections",
                json=payload,
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Error creating connection: {str(e)}")
            if response is not None:
                logging.error(f"Response content: {response.text}")
            raise
