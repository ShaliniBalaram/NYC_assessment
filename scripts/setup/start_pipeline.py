from nifi_controller import NiFiController
import logging
import json
import time
import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def start_processors(nifi, process_group_id):
    """Start all processors in a process group"""
    try:
        # Get list of processors in the process group
        response = requests.get(
            f"{nifi.base_url}/process-groups/{process_group_id}/processors",
            headers=nifi.headers,
            verify=False
        )
        response.raise_for_status()
        processors = response.json()['processors']

        # Start each processor
        for processor in processors:
            processor_id = processor['id']
            
            # Get current processor info
            response = requests.get(
                f"{nifi.base_url}/processors/{processor_id}",
                headers=nifi.headers,
                verify=False
            )
            response.raise_for_status()
            processor_data = response.json()

            # Update the state to RUNNING
            processor_data['component']['state'] = 'RUNNING'
            processor_data['revision']['clientId'] = ""
            processor_data['disconnectedNodeAcknowledged'] = False

            # Start the processor
            response = requests.put(
                f"{nifi.base_url}/processors/{processor_id}",
                json=processor_data,
                headers=nifi.headers,
                verify=False
            )
            response.raise_for_status()
            logger.info(f"Started processor: {processor['component']['name']}")

        logger.info("All processors started successfully")
        return True

    except Exception as e:
        logger.error(f"Error starting processors: {str(e)}")
        if response is not None:
            logger.error(f"Response content: {response.text}")
        return False

def main():
    try:
        nifi = NiFiController()
        logger.info("Connected to NiFi")

        # Get root process group ID
        root_pg_id = nifi.get_root_pg_id()
        logger.info(f"Got root process group ID: {root_pg_id}")

        # Get list of process groups
        response = requests.get(
            f"{nifi.base_url}/process-groups/{root_pg_id}/process-groups",
            headers=nifi.headers,
            verify=False
        )
        response.raise_for_status()
        process_groups = response.json()['processGroups']

        # Find our TLC data pipeline process group
        tlc_pg = next((pg for pg in process_groups if pg['component']['name'] == 'NYC TLC Data Pipeline'), None)
        
        if tlc_pg:
            logger.info(f"Found TLC pipeline process group: {tlc_pg['id']}")
            start_processors(nifi, tlc_pg['id'])
        else:
            logger.error("Could not find NYC TLC Data Pipeline process group")
            return False

        return True

    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        if response is not None:
            logger.error(f"Response content: {response.text}")
        return False

if __name__ == "__main__":
    main()
