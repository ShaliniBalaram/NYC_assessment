from nifi_controller import NiFiController
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_tlc_pipeline():
    try:
        nifi = NiFiController()
        logger.info("Created NiFi controller")
        
        # Create main process group
        pg = nifi.create_process_group("NYC TLC Data Pipeline", {"x": 100, "y": 100})
        logger.info(f"Created process group: {pg.get('id', 'Unknown ID')}")

        # Create InvokeHTTP processor
        invoke_http = nifi.create_processor(
            pg['id'],
            "org.apache.nifi.processors.standard.InvokeHTTP",
            {"x": 100, "y": 100},
            "Fetch TLC Data"
        )
        logger.info(f"Created InvokeHTTP processor: {invoke_http.get('id', 'Unknown ID')}")
        
        # Configure InvokeHTTP processor
        nifi.update_processor(
            invoke_http['id'],
            {
                "HTTP Method": "GET",
                "HTTP URL": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet",
                "SSL Context Service": "",
                "Request Content-Type": "${mime.type}",
                "Response Body Ignored": "false",
                "Response Body Attribute Name": "response.body",
                "Response Body Attribute Size": "10000",
                "Response Header Request Attributes Enabled": "true",
                "Response Redirects Enabled": "True",
                "Response Cookie Strategy": "DISABLED"
            },
            "1 hour"  # Run every hour
        )
        logger.info("Configured InvokeHTTP processor")

        # Create UpdateAttribute processor
        update_attr = nifi.create_processor(
            pg['id'],
            "org.apache.nifi.processors.attributes.UpdateAttribute",
            {"x": 100, "y": 300},
            "Set Attributes"
        )
        logger.info(f"Created UpdateAttribute processor: {update_attr.get('id', 'Unknown ID')}")
        
        # Configure UpdateAttribute processor
        nifi.update_processor(
            update_attr['id'],
            {
                "filename": "${filename:substring(0,${filename:length():minus(8)})}_${now():format('yyyyMMdd')}.parquet"
            },
            "0 sec"
        )
        logger.info("Configured UpdateAttribute processor")

        # Create PutFile processor
        put_file = nifi.create_processor(
            pg['id'],
            "org.apache.nifi.processors.standard.PutFile",
            {"x": 100, "y": 500},
            "Store Data"
        )
        logger.info(f"Created PutFile processor: {put_file.get('id', 'Unknown ID')}")
        
        # Configure PutFile processor
        nifi.update_processor(
            put_file['id'],
            {
                "Directory": "./data",
                "Conflict Resolution Strategy": "replace"
            },
            "0 sec"
        )
        logger.info("Configured PutFile processor")

        # Create connections
        nifi.create_connection(invoke_http['id'], update_attr['id'], pg['id'])
        nifi.create_connection(update_attr['id'], put_file['id'], pg['id'])
        logger.info("Created connections between processors")

        logger.info("Pipeline setup complete!")
        return True
    except Exception as e:
        logger.error(f"Error setting up pipeline: {str(e)}")
        return False

if __name__ == "__main__":
    setup_tlc_pipeline()
