"""Setup script for Superset and Metabase visualizations"""

import logging
import subprocess
import time
from superset_integration import SupersetIntegration
from metabase_integration import MetabaseIntegration

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_docker_running():
    """Check if Docker is running"""
    try:
        subprocess.run(['docker', 'info'], check=True, capture_output=True)
        return True
    except subprocess.CalledProcessError:
        return False

def start_visualization_services():
    """Start Superset and Metabase using Docker"""
    if not check_docker_running():
        logger.error("Docker is not running. Please start Docker and try again.")
        return False

    try:
        # Start Superset
        logger.info("Starting Apache Superset...")
        subprocess.run([
            'docker', 'run', '-d',
            '--name', 'superset',
            '-p', '8088:8088',
            'apache/superset'
        ], check=True)

        # Start Metabase
        logger.info("Starting Metabase...")
        subprocess.run([
            'docker', 'run', '-d',
            '--name', 'metabase',
            '-p', '3000:3000',
            'metabase/metabase'
        ], check=True)

        # Wait for services to be ready
        logger.info("Waiting for services to start...")
        time.sleep(30)  # Give services time to initialize

        return True

    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start visualization services: {str(e)}")
        return False

def setup_visualizations():
    """Set up both Superset and Metabase visualizations"""
    try:
        # Start services
        if not start_visualization_services():
            return False

        # Set up Superset dashboards
        logger.info("Setting up Superset dashboards...")
        superset = SupersetIntegration()
        superset.setup_dashboards()

        # Set up Metabase dashboards
        logger.info("Setting up Metabase dashboards...")
        metabase = MetabaseIntegration()
        metabase.setup_dashboards()

        logger.info("""
        Visualization setup complete!
        
        Access your dashboards at:
        - Superset: http://localhost:8088
        - Metabase: http://localhost:3000
        
        Default credentials:
        Superset:
        - Username: admin
        - Password: admin
        
        Metabase:
        - Email: admin@admin.com
        - Password: metabasepass
        """)

        return True

    except Exception as e:
        logger.error(f"Failed to set up visualizations: {str(e)}")
        return False

if __name__ == "__main__":
    setup_visualizations()
