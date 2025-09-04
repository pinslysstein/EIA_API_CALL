import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class EIAToSnowflakeETL:
    def __init__(self):
        # EIA API Configuration
        self.eia_api_key = os.getenv('EIA_API_KEY', '7c6U3SLuKQAjoa6oUzRW1XAwNKfV2At7OncObI0g')
        self.eia_base_url = "https://api.eia.gov/v2/steo/data/"
        
        # Snowflake Configuration - Your specific values as defaults
        self.snowflake_account = os.getenv('SNOWFLAKE_ACCOUNT', 'IBIEUED-EXA24018')
        self.snowflake_user = os.getenv('SNOWFLAKE_USER', 'API_USER')
        self.snowflake_role = os.getenv('SNOWFLAKE_ROLE', 'API_ROLE')
        self.snowflake_warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'PINSLY_FIVETRAN_COMPUTE')
        self.snowflake_database = os.getenv('SNOWFLAKE_DATABASE', 'PINSLY_RAW')
        self.snowflake_schema = os.getenv('SNOWFLAKE_SCHEMA', 'EIA_RAW')
        # Use rsa_key.p8 in the same directory as this script
        self.snowflake_private_key_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'rsa_key.p8')
        
        # API Parameters
        self.api_params = {
            "frequency": "monthly",
            "data": ["value"],
            "facets": {
                "seriesId": ["DSRTUUS", "DSRTUUS_$", "DSRTUUS_RP_$"]
            },
            "start": None,
            "end": None,
            "sort": [{"column": "period", "direction": "desc"}],
            "offset": 0,
            "length": 5000
        }
    
    def load_private_key(self):
        """Load the private key for Snowflake authentication"""
        try:
            with open(self.snowflake_private_key_path, 'rb') as key_file:
                private_key = load_pem_private_key(
                    key_file.read(),
                    password=None  # No passphrase
                )
            
            private_key_bytes = private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
            return private_key_bytes
        except Exception as e:
            logger.error(f"Error loading private key from {self.snowflake_private_key_path}: {e}")
            raise
    
    def fetch_eia_data(self):
        """Fetch data from EIA API"""
        try:
            # Construct URL with parameters
            params = {
                'api_key': self.eia_api_key,
                'frequency': self.api_params['frequency'],
                'data[0]': 'value',
                'facets[seriesId][]': self.api_params['facets']['seriesId'],
                'sort[0][column]': self.api_params['sort'][0]['column'],
                'sort[0][direction]': self.api_params['sort'][0]['direction'],
                'offset': self.api_params['offset'],
                'length': self.api_params['length']
            }
            
            logger.info("Fetching data from EIA API...")
            logger.info(f"Using API key: {self.eia_api_key[:10]}..." if self.eia_api_key else "No API key found!")
            
            response = requests.get(self.eia_base_url, params=params)
            response.raise_for_status()
            
            data = response.json()
            
            if 'response' in data and 'data' in data['response']:
                df = pd.DataFrame(data['response']['data'])
                logger.info(f"Successfully fetched {len(df)} records from EIA API")
                logger.info(f"Original columns in API response: {list(df.columns)}")
                
                # Clean and standardize column names for Snowflake
                new_columns = []
                for col in df.columns:
                    # Remove quotes, convert to uppercase, replace problematic characters
                    clean_col = col.strip('"').upper()
                    clean_col = clean_col.replace(' ', '_').replace('-', '_').replace('.', '_')
                    # Handle potential reserved words by adding prefix
                    if clean_col in ['VALUE', 'PERIOD', 'DATE', 'TIME', 'YEAR', 'MONTH', 'DAY']:
                        clean_col = f"EIA_{clean_col}"
                    new_columns.append(clean_col)
                
                df.columns = new_columns
                logger.info(f"Cleaned columns: {list(df.columns)}")
                logger.info(f"Sample data: {df.head(2).to_dict()}")
                
                # Add metadata columns
                df['EXTRACTED_TIMESTAMP'] = datetime.now()
                df['SOURCE'] = 'EIA_API'
                
                return df
            else:
                logger.error("No data found in API response")
                logger.error(f"API response: {data}")
                return pd.DataFrame()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data from EIA API: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error fetching EIA data: {e}")
            raise
    
    def connect_to_snowflake(self):
        """Establish connection to Snowflake using key pair authentication"""
        try:
            private_key = self.load_private_key()
            
            connection = snowflake.connector.connect(
                user=self.snowflake_user,
                account=self.snowflake_account,
                private_key=private_key,
                role=self.snowflake_role,
                warehouse=self.snowflake_warehouse,
                database=self.snowflake_database,
                schema=self.snowflake_schema
            )
            
            logger.info("Successfully connected to Snowflake")
            return connection
            
        except Exception as e:
            logger.error(f"Error connecting to Snowflake: {e}")
            raise
    
    def load_data_to_snowflake(self, df, table_name='EIA_STEO_DATA'):
        """Load data to Snowflake table"""
        try:
            connection = self.connect_to_snowflake()
            
            if not df.empty:
                # Drop table and recreate to avoid column mismatch issues
                cursor = connection.cursor()
                cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
                logger.info(f"Dropped existing table {table_name}")
                cursor.close()
                
                # Create table with write_pandas auto_create_table
                success, nchunks, nrows, _ = write_pandas(
                    conn=connection,
                    df=df,
                    table_name=table_name,
                    auto_create_table=True,  # Let pandas create the table
                    overwrite=True,
                    quote_identifiers=False,  # Don't quote identifiers
                    create_temp_table=False
                )
                
                if success:
                    logger.info(f"Successfully loaded {nrows} rows to Snowflake table {table_name}")
                    
                    # Log table structure for verification
                    cursor = connection.cursor()
                    cursor.execute(f"DESCRIBE TABLE {table_name}")
                    table_structure = cursor.fetchall()
                    logger.info(f"Table structure: {table_structure}")
                    cursor.close()
                else:
                    logger.error("Failed to load data to Snowflake")
                    
            else:
                logger.warning("No data to load to Snowflake")
                
        except Exception as e:
            logger.error(f"Error loading data to Snowflake: {e}")
            raise
        finally:
            if connection:
                connection.close()
    
    def run_etl(self):
        """Main ETL process"""
        try:
            logger.info("Starting EIA to Snowflake ETL process...")
            
            # Check if private key file exists
            if not os.path.exists(self.snowflake_private_key_path):
                raise FileNotFoundError(f"Private key file not found: {self.snowflake_private_key_path}")
            
            # Fetch data from EIA API
            df = self.fetch_eia_data()
            
            # Load data to Snowflake
            self.load_data_to_snowflake(df)
            
            logger.info("ETL process completed successfully")
            
        except Exception as e:
            logger.error(f"ETL process failed: {e}")
            raise

if __name__ == "__main__":
    etl = EIAToSnowflakeETL()
    etl.run_etl()
