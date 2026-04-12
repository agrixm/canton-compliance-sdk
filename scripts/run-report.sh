#!/usr/bin/env bash

# ==============================================================================
# Canton Compliance SDK - Report Generation and Validation Script
#
# Description:
#   This script provides a command-line interface to extract trade data from a
#   Canton ledger, generate a regulatory compliance report (e.g., MiFID II, EMIR),
#   and validate it against predefined schemas.
#
# Usage:
#   ./scripts/run-report.sh --party <PARTY_ID> --report-type EMIR --output-file ./emir_report.txt
#
# Prerequisites:
#   - dpm (Daml Package Manager)
#   - jq (command-line JSON processor)
#   - A valid JWT for ledger access (see --token-file)
#   - The project must be built (`dpm build`) to create the necessary DAR file.
# ==============================================================================

set -euo pipefail

# --- Configuration ---
# Default values for command-line arguments
LEDGER_HOST="localhost"
LEDGER_PORT="6866" # Default Canton sandbox gRPC port
PARTY=""
REPORT_TYPE=""
START_DATE=$(date -u -v-1d +%Y-%m-%d) # Default to yesterday UTC
END_DATE=$(date -u +%Y-%m-%d)       # Default to today UTC
OUTPUT_FILE=""
TOKEN_FILE="${HOME}/.daml/token" # Default location for a ledger access token
VALIDATE_ONLY=false
VERBOSE=false

# --- Helper Functions ---

# Print usage information and exit
usage() {
  cat << EOF
Usage: $(basename "$0") [OPTIONS]

Generates and validates regulatory compliance reports from a Canton ledger.

Required Arguments:
  -p, --party <PARTY_ID>        The party for whom to generate the report.
  -t, --report-type <TYPE>      The type of report to generate. Supported: MIFID, EMIR, BASEL3, SFDR.
  -o, --output-file <PATH>      Path to save the generated report.

Optional Arguments:
      --host <HOSTNAME>         Ledger hostname (default: ${LEDGER_HOST}).
      --port <PORT>             Ledger gRPC port (default: ${LEDGER_PORT}).
  -s, --start-date <YYYY-MM-DD> Reporting period start date (default: yesterday).
  -e, --end-date <YYYY-MM-DD>   Reporting period end date (default: today).
      --token-file <PATH>       Path to the JWT token file (default: ${TOKEN_FILE}).
      --validate-only           Validate the specified output file without generating a new report.
  -v, --verbose                 Enable verbose output for debugging.
      --help                    Display this help message and exit.
EOF
  exit 1
}

# Log messages with a timestamp
log() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [INFO]  $*"
}

verbose_log() {
  if [ "$VERBOSE" = true ]; then
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [DEBUG] $*"
  fi
}

error_exit() {
  echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] [ERROR] $*" >&2
  exit 1
}

# Check for required command-line tools
check_dependencies() {
  local missing_deps=0
  for cmd in dpm jq; do
    if ! command -v "$cmd" &> /dev/null; then
      error_exit "Required command '$cmd' not found in PATH. Please install it and try again."
      missing_deps=1
    fi
  done
  verbose_log "All dependencies (dpm, jq) are present."
}

# --- Main Script Logic ---

# Parse command-line arguments using a robust loop
while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--party) PARTY="$2"; shift 2 ;;
    -t|--report-type) REPORT_TYPE=$(echo "$2" | tr '[:lower:]' '[:upper:]'); shift 2 ;;
    -o|--output-file) OUTPUT_FILE="$2"; shift 2 ;;
    --host) LEDGER_HOST="$2"; shift 2 ;;
    --port) LEDGER_PORT="$2"; shift 2 ;;
    -s|--start-date) START_DATE="$2"; shift 2 ;;
    -e|--end-date) END_DATE="$2"; shift 2 ;;
    --token-file) TOKEN_FILE="$2"; shift 2 ;;
    --validate-only) VALIDATE_ONLY=true; shift ;;
    -v|--verbose) VERBOSE=true; shift ;;
    --help) usage ;;
    *) error_exit "Unknown option '$1'" ;;
  esac
done

# --- Input Validation ---
if [ -z "$PARTY" ] || [ -z "$REPORT_TYPE" ] || [ -z "$OUTPUT_FILE" ]; then
  error_exit "Missing required arguments. Use --help for usage."
fi

case "$REPORT_TYPE" in
  MIFID|EMIR|BASEL3|SFDR) ;; # Valid type, continue
  *) error_exit "Invalid report type '${REPORT_TYPE}'. Supported types are: MIFID, EMIR, BASEL3, SFDR." ;;
esac

check_dependencies

# --- Core Functions ---

# Extracts trade data from the ledger by running a Daml Script.
run_extraction_script() {
  log "Starting data extraction for report type ${REPORT_TYPE}..."
  
  [ ! -f "$TOKEN_FILE" ] && error_exit "Token file not found at ${TOKEN_FILE}"
  
  # Assumes a standard DPM project structure
  local dar_path="./.daml/dist/canton-compliance-sdk-0.1.0.dar"
  [ ! -f "$dar_path" ] && error_exit "Project DAR not found at ${dar_path}. Please run 'dpm build' first."
  
  # This script name corresponds to a Daml Script within the project source
  local script_name="Compliance.Scripts.Extract:extractTradeData"
  
  verbose_log "Running Daml Script: ${script_name}"
  verbose_log "Target Ledger: ${LEDGER_HOST}:${LEDGER_PORT}"
  verbose_log "Party: ${PARTY}"
  verbose_log "Period: ${START_DATE} to ${END_DATE}"
  
  # Execute the Daml Script via dpm, passing arguments as a JSON object
  dpm script \
    --dar "${dar_path}" \
    --script-name "${script_name}" \
    --ledger-host "${LEDGER_HOST}" \
    --ledger-port "${LEDGER_PORT}" \
    --access-token-file "${TOKEN_FILE}" \
    --json-api \
    --input-file <(printf '{"party": "%s", "startDate": "%s", "endDate": "%s"}' "$PARTY" "$START_DATE" "$END_DATE")
}

# Transforms extracted JSON data into the final report format.
# In a real SDK, this would invoke a dedicated transformation tool.
transform_to_report_format() {
  local extracted_json="$1"
  log "Transforming extracted data to ${REPORT_TYPE} format..."
  
  verbose_log "Simulating transformation step..."
  
  (
    echo "--- BEGIN ${REPORT_TYPE} REPORT ---"
    echo "Report-Type: ${REPORT_TYPE}"
    echo "Generated-At: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
    echo "Party: ${PARTY}"
    echo "Reporting-Period: ${START_DATE} to ${END_DATE}"
    echo "--- DATA (JSON) ---"
    echo "${extracted_json}" | jq '.' # Pretty-print the JSON data
    echo "--- END ${REPORT_TYPE} REPORT ---"
  ) > "${OUTPUT_FILE}"
  
  log "Report successfully generated at: ${OUTPUT_FILE}"
}

# Validates a report file against its corresponding schema.
# In a real SDK, this would use a schema validator (e.g., XSD for XML reports).
validate_report() {
  local file_to_validate="$1"
  log "Validating report file: ${file_to_validate}"
  
  [ ! -f "$file_to_validate" ] && error_exit "File not found for validation: ${file_to_validate}"

  verbose_log "Simulating validation step for ${REPORT_TYPE} format..."

  if [ -s "$file_to_validate" ] && grep -q "Report-Type: ${REPORT_TYPE}" "$file_to_validate"; then
    log "Validation successful for ${file_to_validate}."
    return 0
  else
    error_exit "Validation failed. File is either empty or does not contain the expected report type header."
    return 1
  fi
}

# --- Main Execution Flow ---
main() {
  if [ "$VALIDATE_ONLY" = true ]; then
    validate_report "${OUTPUT_FILE}"
    exit $?
  fi
  
  # NOTE: The following section is commented out to allow the script to run without a live
  # ledger connection. It uses mock data instead. To use a real ledger, uncomment this
  # section and ensure the ledger is running and accessible.
  #
  # log "Connecting to ledger to extract data..."
  # local extracted_data
  # extracted_data=$(run_extraction_script)
  # if [ -z "$extracted_data" ]; then
  #   error_exit "Data extraction script returned no output. Check ledger connection and script logs."
  # fi
  
  # For demonstration purposes, use mock data.
  log "NOTE: Using mock data for demonstration. To use a live ledger, uncomment the 'run_extraction_script' call."
  local extracted_data
  extracted_data=$(cat <<EOF
[
  {
    "tradeId": "TRADE-2024-001",
    "instrument": "ISIN:US0378331005",
    "quantity": "100.0000000000",
    "price": "175.5000000000",
    "tradeTimestamp": "2024-05-20T14:30:00Z",
    "counterparty": "BankB::1220a4b..."
  },
  {
    "tradeId": "TRADE-2024-002",
    "instrument": "ISIN:DE000BAY0017",
    "quantity": "500.0000000000",
    "price": "52.8000000000",
    "tradeTimestamp": "2024-05-21T09:15:00Z",
    "counterparty": "HedgeFundC::1220c6d..."
  }
]
EOF
)
  
  transform_to_report_format "${extracted_data}"
  
  validate_report "${OUTPUT_FILE}"
  
  log "Process completed successfully."
}

# Execute the main function
main