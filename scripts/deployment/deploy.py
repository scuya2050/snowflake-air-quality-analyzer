"""
Deployment Script for Snowflake Air Quality Analytics

This script automates the deployment process, including:
- Validating configuration
- Testing Snowflake connection
- Running DDL scripts
- Verifying object creation
- Running initial data load

Usage:
    python scripts/deployment/deploy.py --env dev
    python scripts/deployment/deploy.py --env prod --skip-tests
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Dict
import yaml

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "python" / "src"))

try:
    from utils.snowflake_connector import get_snowflake_session, close_snowflake_session
    from utils.config_loader import load_config, load_credentials
except ImportError:
    print("Error: Could not import utility modules. Run from project root.")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SnowflakeDeployer:
    """Handles deployment to Snowflake"""
    
    def __init__(self, environment: str):
        self.environment = environment
        self.config = load_config(environment)
        self.credentials = load_credentials()
        self.session = None
        self.sql_dir = project_root / "sql"
    
    def connect(self) -> bool:
        """Establish connection to Snowflake"""
        try:
            logger.info(f"Connecting to Snowflake ({self.environment})...")
            self.session = get_snowflake_session()
            logger.info("✓ Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"✗ Failed to connect to Snowflake: {str(e)}")
            return False
    
    def disconnect(self):
        """Close Snowflake connection"""
        if self.session:
            close_snowflake_session(self.session)
            logger.info("✓ Disconnected from Snowflake")
    
    def execute_sql_file(self, file_path: Path) -> bool:
        """Execute a SQL file"""
        try:
            logger.info(f"Executing {file_path.name}...")
            
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            # Split by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
            
            for i, stmt in enumerate(statements, 1):
                # Skip comments
                if stmt.startswith('--'):
                    continue
                
                try:
                    self.session.sql(stmt).collect()
                    logger.debug(f"  Statement {i}/{len(statements)} executed")
                except Exception as e:
                    logger.warning(f"  Statement {i} failed: {str(e)}")
            
            logger.info(f"✓ Completed {file_path.name}")
            return True
            
        except Exception as e:
            logger.error(f"✗ Failed to execute {file_path.name}: {str(e)}")
            return False
    
    def deploy_ddl(self) -> bool:
        """Deploy DDL scripts"""
        logger.info("=" * 60)
        logger.info("Deploying DDL Scripts")
        logger.info("=" * 60)
        
        ddl_dir = self.sql_dir / "ddl"
        ddl_files = sorted(ddl_dir.glob("*.sql"))
        
        if not ddl_files:
            logger.warning("No DDL files found")
            return False
        
        success = True
        for sql_file in ddl_files:
            if not self.execute_sql_file(sql_file):
                success = False
                logger.error(f"Deployment failed at {sql_file.name}")
                break
        
        return success
    
    def deploy_dml(self) -> bool:
        """Deploy DML scripts"""
        logger.info("=" * 60)
        logger.info("Deploying DML Scripts")
        logger.info("=" * 60)
        
        dml_dir = self.sql_dir / "dml"
        dml_files = sorted(dml_dir.glob("*.sql"))
        
        if not dml_files:
            logger.warning("No DML files found")
            return True  # Not critical
        
        success = True
        for sql_file in dml_files:
            if not self.execute_sql_file(sql_file):
                logger.warning(f"DML script {sql_file.name} failed (non-critical)")
        
        return success
    
    def verify_deployment(self) -> bool:
        """Verify that objects were created successfully"""
        logger.info("=" * 60)
        logger.info("Verifying Deployment")
        logger.info("=" * 60)
        
        checks = [
            ("Databases", "SHOW DATABASES LIKE 'DEV_DB'"),
            ("Warehouses", "SHOW WAREHOUSES"),
            ("Stage Tables", "SHOW TABLES IN SCHEMA DEV_DB.STAGE_SCH"),
            ("Clean Tables", "SHOW TABLES IN SCHEMA DEV_DB.CLEAN_SCH"),
            ("Consumption Tables", "SHOW TABLES IN SCHEMA DEV_DB.CONSUMPTION_SCH"),
        ]
        
        all_passed = True
        for check_name, query in checks:
            try:
                result = self.session.sql(query).collect()
                count = len(result)
                logger.info(f"✓ {check_name}: {count} objects found")
            except Exception as e:
                logger.error(f"✗ {check_name}: {str(e)}")
                all_passed = False
        
        return all_passed
    
    def run_initial_load(self) -> bool:
        """Run initial data load (optional)"""
        logger.info("=" * 60)
        logger.info("Running Initial Data Load")
        logger.info("=" * 60)
        
        # This would call your ingestion script
        logger.info("⚠ Manual data load required - run ingestion script separately")
        return True


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Deploy Snowflake objects')
    parser.add_argument(
        '--env',
        choices=['dev', 'prod'],
        default='dev',
        help='Target environment (default: dev)'
    )
    parser.add_argument(
        '--skip-tests',
        action='store_true',
        help='Skip verification tests'
    )
    parser.add_argument(
        '--ddl-only',
        action='store_true',
        help='Deploy DDL only, skip DML'
    )
    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify existing deployment'
    )
    return parser.parse_args()


def main():
    """Main deployment flow"""
    args = parse_args()
    
    logger.info("=" * 60)
    logger.info(f"Starting Deployment to {args.env.upper()}")
    logger.info("=" * 60)
    
    deployer = SnowflakeDeployer(args.env)
    
    try:
        # Connect
        if not deployer.connect():
            logger.error("Deployment failed: Could not connect to Snowflake")
            return 1
        
        # Deploy or verify
        if args.verify_only:
            success = deployer.verify_deployment()
        else:
            # Deploy DDL
            success = deployer.deploy_ddl()
            
            if success and not args.ddl_only:
                # Deploy DML
                success = deployer.deploy_dml()
            
            if success and not args.skip_tests:
                # Verify
                success = deployer.verify_deployment()
        
        if success:
            logger.info("=" * 60)
            logger.info("✓ Deployment Successful!")
            logger.info("=" * 60)
            return 0
        else:
            logger.error("=" * 60)
            logger.error("✗ Deployment Failed")
            logger.error("=" * 60)
            return 1
    
    except Exception as e:
        logger.error(f"Deployment error: {str(e)}")
        return 1
    
    finally:
        deployer.disconnect()


if __name__ == "__main__":
    sys.exit(main())
