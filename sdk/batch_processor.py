# sdk/batch_processor.py

"""
Canton Compliance SDK: Parallel Batch Report Processor

This script manages the generation of multiple regulatory reports in parallel.
It reads a batch configuration file, establishes a connection pool to the
Canton Participant Query Store (PQS), and uses a process pool to execute
report generation tasks concurrently.

Each report generation task is handled by a specific module within the
`sdk.formats` package, allowing for easy extension with new report types.

Key Features:
- Concurrent report generation using a process pool for scalability.
- Dynamic loading of report formatters.
- Centralized configuration via a YAML file.
- Robust database connection management using a connection pool.
- Comprehensive logging for monitoring and debugging.
- Support for environment variable substitution in configuration for security.

Usage:
  python -m sdk.batch_processor --config /path/to/reports.yaml
"""

import argparse
import concurrent.futures
import importlib
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict

import psycopg2
import yaml
from psycopg2.pool import ThreadedConnectionPool

# --- Constants ---
DEFAULT_CONCURRENCY = os.cpu_count() or 4
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(processName)s - %(message)s"
CONFIG_TEMPLATE = """
# Configuration for the Canton Compliance SDK Batch Processor
database:
  # Participant Query Store (PQS) connection details
  # Supports environment variable substitution, e.g., ${PQS_HOST}
  host: "localhost"
  port: 5432
  dbname: "pqs"
  user: "pqs_user"
  password: "${PQS_PASSWORD}" # Use env var for password
  min_conn: 1
  max_conn: 10

settings:
  # Number of parallel processes to use for report generation
  concurrency: 4
  # Base directory for report outputs
  output_dir: "./reports_output"
  # Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL
  log_level: "INFO"

reports:
  - name: "Sample SFDR PAI Report - Q4 2024"
    type: "sfdr" # Corresponds to sdk.formats.sfdr module
    enabled: true
    parameters:
      reporting_entity: "MyBank::1220xxxxxxxx"
      start_date: "2024-10-01"
      end_date: "2024-12-31"
    output_file: "sfdr_pai_2024_q4.json"

  - name: "Sample Basel III COLL5 Report"
    type: "coll5" # Corresponds to sdk.formats.coll5 module
    enabled: true
    parameters:
      participant_id: "MyBank::1220xxxxxxxx"
      as_of_date: "2025-01-15"
    output_file: "coll5_daily_2025_01_15.xml"
"""

# --- Logging Setup ---
logger = logging.getLogger("BatchProcessor")

def setup_logging(level: str = "INFO"):
    """Configures the root logger for the application."""
    log_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(level=log_level, format=LOG_FORMAT, stream=sys.stdout)
    logger.info(f"Logging configured at level {level.upper()}")

# --- Configuration Loading ---
def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Loads, validates, and processes the YAML configuration file.
    Supports environment variable substitution.
    """
    if not config_path.is_file():
        logger.error(f"Configuration file not found at {config_path}")
        logger.info("A template config file can be generated with --generate-config")
        sys.exit(1)

    logger.info(f"Loading configuration from {config_path}")
    with open(config_path, 'r') as f:
        content = f.read()
        expanded_content = os.path.expandvars(content)
        config = yaml.safe_load(expanded_content)

    required_keys = ["database", "settings", "reports"]
    for key in required_keys:
        if key not in config:
            raise ValueError(f"Missing required top-level key '{key}' in config file.")

    return config

# --- Worker Function ---
def generate_report(
    db_pool: ThreadedConnectionPool,
    report_config: Dict[str, Any],
    output_dir: Path
) -> Dict[str, Any]:
    """
    The main worker function executed by each process in the pool.
    It generates a single report.

    Args:
        db_pool: The database connection pool.
        report_config: Configuration dictionary for the specific report.
        output_dir: The base directory to write the report output.

    Returns:
        A dictionary containing the report name and status.
    """
    report_name = report_config.get("name", "Unnamed Report")
    report_type = report_config.get("type")
    output_file = report_config.get("output_file")
    start_time = time.monotonic()

    logger.info(f"Starting report generation for '{report_name}'")

    if not report_type or not output_file:
        return {
            "name": report_name,
            "status": "FAILED",
            "reason": "Missing 'type' or 'output_file' in report configuration.",
            "duration": time.monotonic() - start_time,
        }

    conn = None
    try:
        module_name = f"sdk.formats.{report_type}"
        formatter_module = importlib.import_module(module_name)

        if not hasattr(formatter_module, "run_report"):
            raise AttributeError(f"Module {module_name} does not have a 'run_report' function.")

        conn = db_pool.getconn()
        logger.debug(f"Acquired DB connection for '{report_name}'")

        output_path = output_dir / output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)

        formatter_module.run_report(
            db_conn=conn,
            parameters=report_config.get("parameters", {}),
            output_path=output_path
        )

        conn.commit()
        duration = time.monotonic() - start_time
        logger.info(f"Successfully generated report '{report_name}' in {duration:.2f}s. Output: {output_path}")

        return {
            "name": report_name,
            "status": "SUCCESS",
            "output_path": str(output_path),
            "duration": duration,
        }

    except Exception as e:
        if conn:
            conn.rollback()
        duration = time.monotonic() - start_time
        logger.error(f"Failed to generate report '{report_name}' after {duration:.2f}s", exc_info=True)
        return {
            "name": report_name,
            "status": "FAILED",
            "reason": str(e),
            "duration": duration,
        }
    finally:
        if conn:
            db_pool.putconn(conn)
            logger.debug(f"Released DB connection for '{report_name}'")


# --- Main Orchestrator ---
def main():
    """Main function to orchestrate the batch processing."""
    parser = argparse.ArgumentParser(description="Canton Compliance SDK Batch Report Processor.")
    parser.add_argument(
        "-c", "--config",
        type=Path,
        required=True,
        help="Path to the YAML configuration file for the batch job."
    )
    parser.add_argument(
        "--generate-config",
        action="store_true",
        help="Generate a template config file at the path specified by --config and exit."
    )
    args = parser.parse_args()

    if args.generate_config:
        config_path = args.config
        try:
            config_path.parent.mkdir(parents=True, exist_ok=True)
            with open(config_path, 'w') as f:
                f.write(CONFIG_TEMPLATE)
            print(f"Generated template config file at: {config_path}")
            sys.exit(0)
        except IOError as e:
            print(f"Error writing template config file: {e}", file=sys.stderr)
            sys.exit(1)

    try:
        config = load_config(args.config)
    except (ValueError, yaml.YAMLError) as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        sys.exit(1)

    settings = config.get("settings", {})
    db_config = config.get("database", {})

    setup_logging(settings.get("log_level", "INFO"))

    concurrency = settings.get("concurrency", DEFAULT_CONCURRENCY)
    output_dir = Path(settings.get("output_dir", "./reports_output"))
    output_dir.mkdir(parents=True, exist_ok=True)

    db_pool = None
    try:
        logger.info(f"Initializing database connection pool for {db_config.get('host')}:{db_config.get('port')}")
        db_pool = ThreadedConnectionPool(
            minconn=db_config.get("min_conn", 1),
            maxconn=db_config.get("max_conn", concurrency),
            host=db_config.get("host"),
            port=db_config.get("port"),
            dbname=db_config.get("dbname"),
            user=db_config.get("user"),
            password=db_config.get("password"),
        )

        reports_to_run = [r for r in config.get("reports", []) if r.get("enabled", True)]
        if not reports_to_run:
            logger.warning("No enabled reports found in the configuration file. Exiting.")
            return

        logger.info(f"Starting batch processing for {len(reports_to_run)} reports with concurrency={concurrency}")
        total_start_time = time.monotonic()

        results = []
        with concurrent.futures.ProcessPoolExecutor(max_workers=concurrency) as executor:
            future_to_report = {
                executor.submit(generate_report, db_pool, report_conf, output_dir): report_conf
                for report_conf in reports_to_run
            }

            for future in concurrent.futures.as_completed(future_to_report):
                report_conf = future_to_report[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    logger.error(f"Report '{report_conf.get('name')}' generated an exception: {exc}")
                    results.append({
                        "name": report_conf.get('name'),
                        "status": "CRASHED",
                        "reason": str(exc),
                    })

        total_duration = time.monotonic() - total_start_time
        logger.info(f"Batch processing finished in {total_duration:.2f}s")

        # --- Summary ---
        success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
        failed_count = len(results) - success_count

        print("\n--- Batch Processing Summary ---")
        print(f"Total reports processed: {len(results)}")
        print(f"  - Successful: {success_count}")
        print(f"  - Failed:     {failed_count}")
        print(f"Total time taken: {total_duration:.2f} seconds")

        if failed_count > 0:
            print("\n--- Failed Reports ---")
            for r in results:
                if r['status'] != 'SUCCESS':
                    print(f"  - Name:   {r['name']}")
                    print(f"    Status: {r['status']}")
                    print(f"    Reason: {r['reason']}")
            sys.exit(1)

    except psycopg2.Error as db_err:
        logger.critical(f"Database connection error: {db_err}", exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during batch processing: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db_pool:
            db_pool.closeall()
            logger.info("Database connection pool closed.")


if __name__ == "__main__":
    main()