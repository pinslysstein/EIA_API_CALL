EIA API to Snowflake ETL Pipeline
This project provides a daily ETL pipeline that fetches energy data from the EIA (Energy Information Administration) API and loads it into Snowflake using key pair authentication.

Features
Fetches monthly STEO (Short-Term Energy Outlook) data from EIA API
Uses Snowflake key pair authentication for secure connections
Automated daily refresh via GitHub Actions
Comprehensive logging and error handling
Full refresh strategy (can be modified for incremental loads)
Setup Instructions
1. Snowflake Setup
Generate Key Pair for Authentication
bash
# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt

# Generate public key
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
Configure Snowflake User
sql
-- In Snowflake, run these commands as ACCOUNTADMIN
ALTER USER your_username SET RSA_PUBLIC_KEY='<public_key_content>';

-- Grant necessary privileges
GRANT ROLE your_role TO USER your_username;
GRANT USAGE ON WAREHOUSE your_warehouse TO ROLE your_role;
GRANT USAGE ON DATABASE your_database TO ROLE your_role;
GRANT USAGE ON SCHEMA your_database.your_schema TO ROLE your_role;
GRANT CREATE TABLE ON SCHEMA your_database.your_schema TO ROLE your_role;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA your_database.your_schema TO ROLE your_role;
2. GitHub Repository Setup
Required Files
eia_snowflake_etl.py - Main ETL script
requirements.txt - Python dependencies
.github/workflows/eia_etl.yml - GitHub Actions workflow
README.md - This file
GitHub Secrets Configuration
Go to your repository settings and add these secrets:

EIA API Configuration:

EIA_API_KEY: Your EIA API key (7c6U3SLuKQAjoa6oUzRW1XAwNKfV2At7OncObI0g)
Snowflake Configuration:

SNOWFLAKE_ACCOUNT: Your Snowflake account identifier (e.g., abc12345.us-east-1)
SNOWFLAKE_USER: Your Snowflake username
SNOWFLAKE_ROLE: Your Snowflake role
SNOWFLAKE_WAREHOUSE: Your Snowflake warehouse name
SNOWFLAKE_DATABASE: Your Snowflake database name
SNOWFLAKE_SCHEMA: Your Snowflake schema name (optional, defaults to PUBLIC)
SNOWFLAKE_PRIVATE_KEY: Content of your private key file (rsa_key.p8)
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE: Passphrase for private key (if encrypted)
3. Local Development Setup
bash
# Clone the repository
git clone <your-repo-url>
cd <your-repo-name>

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables for local testing
export EIA_API_KEY="7c6U3SLuKQAjoa6oUzRW1XAwNKfV2At7OncObI0g"
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USER="your-user"
export SNOWFLAKE_ROLE="your-role"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_SCHEMA="your-schema"
export SNOWFLAKE_PRIVATE_KEY_PATH="./rsa_key.p8"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your-passphrase"

# Run the ETL script
python eia_snowflake_etl.py
4. Data Schema
The ETL creates a table EIA_STEO_DATA with the following structure:

sql
CREATE TABLE EIA_STEO_DATA (
    period STRING,              -- Time period (YYYY-MM format)
    seriesId STRING,           -- EIA series identifier
    value FLOAT,               -- Data value
    units STRING,              -- Units of measurement
    extracted_timestamp TIMESTAMP_NTZ,  -- When data was extracted
    source STRING,             -- Data source identifier
    created_date TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
5. Monitoring and Troubleshooting
GitHub Actions Monitoring
Check the "Actions" tab in your GitHub repository
Review logs for any failed runs
The workflow runs daily at 6 AM UTC
Common Issues
Authentication Failures: Verify your private key format and Snowflake user configuration
API Rate Limits: EIA API has rate limits; the script includes error handling
Missing Environment Variables: Ensure all required GitHub secrets are set
Customization Options
Schedule: Modify the cron expression in the workflow file
Data Refresh: Change from full refresh to incremental by modifying the truncate logic
Additional Series: Add more series IDs to the api_params['facets']['seriesId'] list
Table Name: Modify the table name in the load_data_to_snowflake method
6. API Information
EIA API Endpoint: https://api.eia.gov/v2/steo/data/ Series IDs being fetched:

DSRTUUS: Distillate fuel oil stocks, US total
DSRTUUS_$: Related series
DSRTUUS_RP_$: Related series
API Parameters:

Frequency: Monthly
Sort: By period (descending)
Limit: 5000 records
For more information about EIA API, visit: https://www.eia.gov/opendata/

Support
If you encounter issues:

Check the GitHub Actions logs
Verify all secrets are correctly set
Test the connection locally first
Review Snowflake permissions
