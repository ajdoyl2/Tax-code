#!/bin/bash
#
# Archon MCP Stop Hook
#
# This hook runs when Claude stops and checks if Archon tests failed.
# If tests failed, it stops Docker services and provides troubleshooting info.
#

set -e

# Read hook input from stdin
INPUT=$(cat)

# Get working directory from hook context
CWD=$(echo "$INPUT" | jq -r '.cwd // ""')
ARCHON_DIR="${CWD}/archon"

# Check if archon directory exists
if [ ! -d "$ARCHON_DIR" ]; then
    exit 0
fi

# Check for test failure indicators in transcript
TRANSCRIPT_PATH=$(echo "$INPUT" | jq -r '.transcript_path // ""')

TESTS_FAILED=false
if [ -f "$TRANSCRIPT_PATH" ]; then
    # Look for test failure patterns
    if grep -qiE "(FAILED|AssertionError|test.*fail|ERROR.*test)" "$TRANSCRIPT_PATH" 2>/dev/null; then
        TESTS_FAILED=true
    fi
fi

# If tests failed, stop Archon and show troubleshooting
if [ "$TESTS_FAILED" = true ]; then
    echo "=========================================="
    echo "  ARCHON MCP TEST FAILURE DETECTED"
    echo "=========================================="
    echo ""
    echo "Stopping Archon Docker services..."

    cd "$ARCHON_DIR"

    # Stop Docker services
    if command -v docker &> /dev/null; then
        docker compose down 2>/dev/null || docker-compose down 2>/dev/null || true
        echo "Docker services stopped."
    fi

    echo ""
    echo "TROUBLESHOOTING CHECKLIST:"
    echo "--------------------------"
    echo "1. Check .env configuration:"
    echo "   cat archon/.env | grep -E '^(SUPABASE|HOST|PORT)'"
    echo ""
    echo "2. Verify Supabase credentials:"
    echo "   - SUPABASE_URL should be your project URL"
    echo "   - SUPABASE_SERVICE_KEY must be the SERVICE ROLE key (not anon)"
    echo ""
    echo "3. Run database migrations:"
    echo "   Execute archon/migration/complete_setup.sql in Supabase SQL Editor"
    echo ""
    echo "4. Check Docker logs:"
    echo "   cd archon && docker compose logs -f"
    echo ""
    echo "5. Restart services:"
    echo "   cd archon && docker compose up --build -d"
    echo ""
    echo "6. Verify services are running:"
    echo "   docker compose ps"
    echo ""
    echo "7. Test MCP endpoint manually:"
    echo "   curl http://localhost:8051/health"
    echo ""
    echo "=========================================="
fi

exit 0
