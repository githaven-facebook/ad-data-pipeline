#!/usr/bin/env bash
# Run all tests with coverage reporting
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== Ad Data Pipeline Test Runner ==="
echo "Project root: ${PROJECT_ROOT}"
cd "${PROJECT_ROOT}"

# Parse arguments
RUN_INTEGRATION=false
FAST_MODE=false
COVERAGE_REPORT=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --integration)
            RUN_INTEGRATION=true
            shift
            ;;
        --fast)
            FAST_MODE=true
            shift
            ;;
        --no-coverage)
            COVERAGE_REPORT=false
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build pytest arguments
PYTEST_ARGS=(
    "tests/"
    "--tb=short"
    "-v"
)

if [ "${FAST_MODE}" = true ]; then
    PYTEST_ARGS+=("-m" "not slow and not integration")
elif [ "${RUN_INTEGRATION}" = false ]; then
    PYTEST_ARGS+=("-m" "not integration")
fi

if [ "${COVERAGE_REPORT}" = true ]; then
    PYTEST_ARGS+=(
        "--cov=src/ad_data_pipeline"
        "--cov-report=term-missing"
        "--cov-report=xml:coverage.xml"
        "--cov-report=html:htmlcov"
        "--cov-fail-under=70"
    )
fi

echo ""
echo "Running: pytest ${PYTEST_ARGS[*]}"
echo ""

python -m pytest "${PYTEST_ARGS[@]}"

EXIT_CODE=$?

if [ "${COVERAGE_REPORT}" = true ] && [ -f coverage.xml ]; then
    echo ""
    echo "Coverage report written to: ${PROJECT_ROOT}/htmlcov/index.html"
fi

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo "All tests passed!"
else
    echo ""
    echo "Tests failed with exit code: ${EXIT_CODE}"
fi

exit $EXIT_CODE
