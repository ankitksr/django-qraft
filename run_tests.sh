#!/usr/bin/env bash
# Simple test runner script for django-qraft

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Django-Qraft Test Runner${NC}"
echo ""

# Check if uv is available
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}Warning: uv not found. Install it with: curl -LsSf https://astral.sh/uv/install.sh | sh${NC}"
    echo "Falling back to pytest..."
    RUNNER="pytest"
else
    RUNNER="uv run pytest"
fi

# Parse command line arguments
COVERAGE=false
PARALLEL=false
VERBOSE=false
PATTERN=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --cov|--coverage)
            COVERAGE=true
            shift
            ;;
        -n|--parallel)
            PARALLEL=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -k)
            PATTERN="$2"
            shift 2
            ;;
        *)
            # Pass through unknown arguments
            EXTRA_ARGS="$EXTRA_ARGS $1"
            shift
            ;;
    esac
done

# Build command
CMD="$RUNNER"

if [ "$COVERAGE" = true ]; then
    CMD="$CMD --cov=qraft --cov-report=html --cov-report=term"
fi

if [ "$PARALLEL" = true ]; then
    CMD="$CMD -n auto"
fi

if [ "$VERBOSE" = true ]; then
    CMD="$CMD -vv -s"
fi

if [ -n "$PATTERN" ]; then
    CMD="$CMD -k $PATTERN"
fi

CMD="$CMD $EXTRA_ARGS"

# Run tests
echo "Running: $CMD"
echo ""
eval $CMD

# Show coverage report if generated
if [ "$COVERAGE" = true ]; then
    echo ""
    echo -e "${GREEN}Coverage report generated at: htmlcov/index.html${NC}"
    echo "Open it with: open htmlcov/index.html"
fi
