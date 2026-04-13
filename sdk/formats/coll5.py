# sdk/formats/coll5.py

"""
FCA COLL 5 Collateral Reporting Formatter.

This script connects to a Canton Participant Query Store (PQS), extracts data
on collateral holdings for a specific party, and formats it into the CSV
format required for FCA COLL 5 reporting.

The script is designed to be plug-and-play for any Canton participant using a
standard set of Daml models for collateral management. It queries the active
contract set for collateral pledges and their underlying instrument details.

Usage:
  python sdk/formats/coll5.py \
    --pqs-host <pqs_db_host> \
    --pqs-port <pqs_db_port> \
    --pqs-db <pqs_db_name> \
    --pqs-user <pqs_user> \
    --party <reporting_party_id> \
    --reporting-date <YYYY-MM-DD> \
    --output-file <path/to/report.csv>

Environment variables can be used for PQS connection details (PQS_HOST, PQS_PORT,
PQS_DB, PQS_USER, PGPASSWORD).
"""

import argparse
import csv
import datetime
import decimal
import logging
import os
import sys

# Attempt to import psycopg2, providing a helpful error message if it's missing.
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print(
        "Error: The 'psycopg2-binary' package is required for database connectivity.",
        "Please install it using: pip install psycopg2-binary",
        file=sys.stderr
    )
    sys.exit(1)

# Configure logging for clear output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Define the headers for the COLL 5 CSV report, based on common regulatory requirements.
# These fields represent a standardised view of collateral held by a fund.
COLL5_CSV_HEADERS = [
    'ReportingFirmLEI',
    'ReportDate',
    'FundID',
    'CollateralProviderLEI',
    'CollateralAssetISIN',
    'CollateralType',
    'IssuerLEI',
    'CountryOfIssuer',
    'Currency',
    'NominalAmount',
    'MarketValue',
    'HaircutPercentage',
    'CollateralValueAfterHaircut',
    'MaturityDate',
]

# --- PQS SQL Query ---
# This query is designed to run against a Canton PQS PostgreSQL database.
# It assumes a specific set of Daml templates for collateral management, which
# would be part of the shared financial models on the Canton network.
#
# Assumed Templates:
# - `Collateral.V1.Pledge`: A contract representing a collateral pledge.
#   - `provider`: Party (LEI) providing the collateral.
#   - `receiver`: Party (LEI) receiving the collateral (the reporting firm).
#   - `fundId`: Text identifier for the fund receiving the collateral.
#   - `assetId`: Text identifier linking to the instrument.
#   - `quantity`: Decimal, the amount/quantity of the asset pledged.
#   - `haircut`: Optional Decimal, the haircut applied to the collateral's value.
# - `Instrument.Security.V1.FixedIncome`: A contract for a fixed-income security.
#   - `id`: Text, a unique identifier for the instrument.
#   - `isin`: Text, the ISIN of the security.
#   - `issuer`: Party (LEI) of the security issuer.
#   - `issuerCountry`: Text, ISO 3166-1 alpha-2 country code.
#   - `currency`: Text, ISO 4217 currency code.
#   - `maturityDate`: Date.
# - `Market.Data.V1.Price`: A contract holding the price for an instrument.
#   - `instrumentId`: Text, identifier linking to the instrument.
#   - `price`: Decimal, the clean price of the instrument.

PQS_COLLATERAL_QUERY = """
WITH prices AS (
    -- Subquery to get the latest price for each instrument as of the reporting date.
    -- This ensures the valuation is based on the most recent available data.
    SELECT DISTINCT ON (payload ->> 'instrumentId')
        payload ->> 'instrumentId' AS instrument_id,
        (payload ->> 'price')::decimal AS price
    FROM creates('Market.Data.V1.Price')
    WHERE
        -- Filter by party visibility and timestamp
        can_read_as($1::text)
        AND offset_timestamp <= ($2::date + interval '1 day') -- Inclusive of the reporting date
    ORDER BY
        payload ->> 'instrumentId', offset_timestamp DESC
)
SELECT
    pledge.payload ->> 'receiver' AS reporting_firm_id,
    pledge.payload ->> 'fundId' AS fund_id,
    pledge.payload ->> 'provider' AS collateral_provider_lei,
    instr.payload ->> 'isin' AS isin,
    'FIXED_INCOME' AS collateral_type, -- Example: could be dynamic based on template name
    instr.payload ->> 'issuer' AS issuer_lei,
    instr.payload ->> 'issuerCountry' AS country_of_issuer,
    instr.payload ->> 'currency' AS currency,
    (pledge.payload ->> 'quantity')::decimal AS nominal_amount,
    -- Market value is calculated as quantity * price
    ((pledge.payload ->> 'quantity')::decimal * p.price) AS market_value,
    COALESCE((pledge.payload ->> 'haircut')::decimal, 0.0) AS haircut,
    instr.payload ->> 'maturityDate' as maturity_date
FROM
    -- Query the active contract set (ACS) for pledges visible to the reporting party
    active('Collateral.V1.Pledge', $1::text) AS pledge
JOIN
    active('Instrument.Security.V1.FixedIncome', $1::text) AS instr
    ON (pledge.payload ->> 'assetId' = instr.payload ->> 'id')
JOIN
    prices p ON (instr.payload ->> 'id' = p.instrument_id)
WHERE
    -- Ensure we are reporting on collateral received by the specified party
    pledge.payload ->> 'receiver' = $1::text;
"""


def fetch_collateral_data(db_conn_info, party, reporting_date):
    """
    Connects to the PQS and fetches collateral data for the given party and date.

    Args:
        db_conn_info (dict): Database connection parameters.
        party (str): The Daml party ID of the reporting firm.
        reporting_date (str): The 'YYYY-MM-DD' date for the report.

    Returns:
        list: A list of dictionaries, each representing a row of collateral data.
    """
    logging.info(f"Connecting to PQS at {db_conn_info['host']}:{db_conn_info['port']}...")
    try:
        with psycopg2.connect(**db_conn_info) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                logging.info(f"Executing PQS query for party '{party}' on date '{reporting_date}'")
                cur.execute(PQS_COLLATERAL_QUERY, (party, reporting_date))
                results = [dict(row) for row in cur.fetchall()]
                logging.info(f"Fetched {len(results)} collateral records from PQS.")
                return results
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An error occurred while querying the PQS database: {e}")
        sys.exit(1)


def transform_data_for_report(raw_data, reporting_date):
    """
    Transforms raw data from PQS into the final COLL 5 report format, performing
    calculations and ensuring correct data types.

    Args:
        raw_data (list): The list of dictionaries from fetch_collateral_data.
        reporting_date (str): The 'YYYY-MM-DD' date for the report.

    Returns:
        list: A list of dictionaries formatted for the CSV report.
    """
    transformed_records = []
    for record in raw_data:
        try:
            market_value = record.get('market_value', decimal.Decimal('0.0'))
            haircut = record.get('haircut', decimal.Decimal('0.0'))

            # Calculate the value of the collateral after applying the haircut
            value_after_haircut = market_value * (decimal.Decimal('1.0') - haircut)

            transformed_record = {
                'ReportingFirmLEI': record.get('reporting_firm_id', ''),
                'ReportDate': reporting_date,
                'FundID': record.get('fund_id', 'UNKNOWN_FUND'),
                'CollateralProviderLEI': record.get('collateral_provider_lei', ''),
                'CollateralAssetISIN': record.get('isin', ''),
                'CollateralType': record.get('collateral_type', ''),
                'IssuerLEI': record.get('issuer_lei', ''),
                'CountryOfIssuer': record.get('country_of_issuer', ''),
                'Currency': record.get('currency', ''),
                'NominalAmount': f"{record.get('nominal_amount', decimal.Decimal('0.0')):.2f}",
                'MarketValue': f"{market_value:.2f}",
                'HaircutPercentage': f"{haircut:.4f}",
                'CollateralValueAfterHaircut': f"{value_after_haircut:.2f}",
                'MaturityDate': record.get('maturity_date', ''),
            }
            transformed_records.append(transformed_record)
        except (TypeError, decimal.InvalidOperation, KeyError) as e:
            logging.warning(f"Skipping record due to data transformation error: {e}. Record: {record}")
            continue

    return transformed_records


def generate_csv_report(report_data, output_file):
    """
    Writes the transformed data to a CSV file.

    Args:
        report_data (list): The final list of dictionaries to write.
        output_file (str): The path to the output CSV file.
    """
    logging.info(f"Generating COLL 5 CSV report at '{output_file}'...")
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=COLL5_CSV_HEADERS)
            writer.writeheader()
            writer.writerows(report_data)
        logging.info(f"Successfully generated CSV report with {len(report_data)} rows.")
    except IOError as e:
        logging.error(f"Failed to write to output file '{output_file}': {e}")
        sys.exit(1)


def main():
    """Main execution function to parse arguments and run the report generation."""
    parser = argparse.ArgumentParser(description="Generate FCA COLL 5 collateral reports from a Canton PQS.")

    # PQS Connection Arguments
    parser.add_argument('--pqs-host', default=os.environ.get('PQS_HOST', 'localhost'), help="PQS database host.")
    parser.add_argument('--pqs-port', type=int, default=os.environ.get('PQS_PORT', 5432), help="PQS database port.")
    parser.add_argument('--pqs-db', default=os.environ.get('PQS_DB', 'pqs'), help="PQS database name.")
    parser.add_argument('--pqs-user', default=os.environ.get('PQS_USER', 'pqs_user'), help="PQS database user.")
    parser.add_argument('--pqs-password', help="PQS database password. Recommended: use PGPASSWORD env var.")

    # Reporting Arguments
    parser.add_argument('--party', required=True, help="Daml party ID of the reporting firm (e.g., 'FundManager::1220...').")
    parser.add_argument('--reporting-date', default=datetime.date.today().strftime('%Y-%m-%d'), help="Report date (YYYY-MM-DD). Defaults to today.")
    parser.add_argument('--output-file', required=True, help="Path to the output CSV file.")

    args = parser.parse_args()

    # Validate reporting date format
    try:
        datetime.datetime.strptime(args.reporting_date, '%Y-%m-%d')
    except ValueError:
        logging.error("Invalid date format for --reporting-date. Please use YYYY-MM-DD.")
        sys.exit(1)

    # Securely retrieve database password
    db_password = args.pqs_password or os.environ.get('PGPASSWORD')
    if not db_password:
        logging.error("PQS password not provided. Use --pqs-password or set PGPASSWORD environment variable.")
        sys.exit(1)

    db_conn_info = {
        'host': args.pqs_host,
        'port': args.pqs_port,
        'dbname': args.pqs_db,
        'user': args.pqs_user,
        'password': db_password,
    }

    # Execute the reporting workflow
    raw_data = fetch_collateral_data(db_conn_info, args.party, args.reporting_date)

    if not raw_data:
        logging.warning("No collateral data found for the specified criteria. An empty report will be generated.")

    report_data = transform_data_for_report(raw_data, args.reporting_date)
    generate_csv_report(report_data, args.output_file)


if __name__ == '__main__':
    main()