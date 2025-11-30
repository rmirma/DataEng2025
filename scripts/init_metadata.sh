#!/bin/bash
#
# Master initialization script for pre-populating metadata
# Run this after all services are up and healthy
#
# Usage: ./scripts/init_metadata.sh [--openmetadata] [--superset] [--all]
#

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

load_date_csv() {
    log_info "Loading date CSV into ClickHouse..."
    
    # Check if ClickHouse is running
    if ! curl -s "http://localhost:8123/ping" > /dev/null 2>&1; then
        log_error "ClickHouse is not running at http://localhost:8123"
        return 1
    fi
    
    # Check if date_raw already has data
    ROW_COUNT=$(curl -s "http://localhost:8123/?user=clickhouse&password=clickhouse" \
        --data-binary "SELECT count(*) FROM bronze.date_raw" 2>/dev/null || echo "0")
    
    if [ "$ROW_COUNT" -gt "0" ] 2>/dev/null; then
        log_info "Date table already has $ROW_COUNT rows, skipping load"
        return 0
    fi
    
    # Load the CSV
    if [ -f "$PROJECT_ROOT/data/2024-dates.csv" ]; then
        cat "$PROJECT_ROOT/data/2024-dates.csv" | curl -s \
            "http://localhost:8123/?user=clickhouse&password=clickhouse&query=INSERT+INTO+bronze.date_raw+(Date,WeekDay,HolidayDesc,HolidayInd)+FORMAT+CSVWithNames" \
            --data-binary @-
        
        NEW_COUNT=$(curl -s "http://localhost:8123/?user=clickhouse&password=clickhouse" \
            --data-binary "SELECT count(*) FROM bronze.date_raw" 2>/dev/null || echo "0")
        log_info "Loaded $NEW_COUNT date records into bronze.date_raw"
    else
        log_warn "Date CSV not found at $PROJECT_ROOT/data/2024-dates.csv"
        return 1
    fi
}

check_python() {
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is required but not installed"
        exit 1
    fi
    
    # Check for requests library
    if ! python3 -c "import requests" 2>/dev/null; then
        log_warn "Installing requests library..."
        pip3 install requests --quiet
    fi
}

init_openmetadata() {
    log_info "Initializing OpenMetadata..."
    
    # Check if OpenMetadata is running
    if ! curl -s http://localhost:8585/api/v1/system/version > /dev/null 2>&1; then
        log_error "OpenMetadata is not running at http://localhost:8585"
        log_warn "Start it with: docker compose --profile openmetadata up -d"
        return 1
    fi
    
    python3 "$PROJECT_ROOT/openmetadata/init/init_openmetadata.py"
}

init_superset() {
    log_info "Initializing Superset..."
    
    # Check if Superset is running
    if ! curl -s http://localhost:8088/health > /dev/null 2>&1; then
        log_error "Superset is not running at http://localhost:8088"
        log_warn "Start it with: docker compose --profile superset up -d"
        return 1
    fi
    
    python3 "$PROJECT_ROOT/superset/assets/init_superset.py"
}

show_help() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Initialize metadata for data catalog and visualization tools."
    echo ""
    echo "Options:"
    echo "  --openmetadata    Initialize OpenMetadata with table descriptions"
    echo "  --superset        Initialize Superset with ClickHouse connection"
    echo "  --all             Initialize all services"
    echo "  -h, --help        Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --all              # Initialize everything"
    echo "  $0 --openmetadata     # Just OpenMetadata"
    echo "  $0 --superset         # Just Superset"
}

# Main
main() {
    if [ $# -eq 0 ]; then
        show_help
        exit 0
    fi
    
    check_python
    
    INIT_OM=false
    INIT_SS=false
    
    while [ $# -gt 0 ]; do
        case "$1" in
            --openmetadata)
                INIT_OM=true
                shift
                ;;
            --superset)
                INIT_SS=true
                shift
                ;;
            --all)
                INIT_OM=true
                INIT_SS=true
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                exit 1
                ;;
        esac
    done
    
    echo "=============================================="
    echo "  Metadata Initialization"
    echo "=============================================="
    
    # Always load date CSV first (idempotent)
    load_date_csv || log_warn "Date CSV loading had issues"
    echo ""
    
    if [ "$INIT_OM" = true ]; then
        init_openmetadata || log_warn "OpenMetadata initialization had issues"
        echo ""
    fi
    
    if [ "$INIT_SS" = true ]; then
        init_superset || log_warn "Superset initialization had issues"
        echo ""
    fi
    
    log_info "Initialization complete!"
}

main "$@"
