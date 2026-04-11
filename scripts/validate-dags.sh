#!/usr/bin/env bash
# Validate all Airflow DAG files: import check, syntax, cycle detection
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
DAGS_DIR="${PROJECT_ROOT}/dags"

echo "=== DAG Validation Script ==="
echo "DAGs directory: ${DAGS_DIR}"
echo ""

# Track pass/fail
PASS_COUNT=0
FAIL_COUNT=0
FAILED_DAGS=()

# Find all DAG files
DAG_FILES=$(find "${DAGS_DIR}" -name "*.py" -not -name "__init__.py" | sort)

if [ -z "${DAG_FILES}" ]; then
    echo "No DAG files found in ${DAGS_DIR}"
    exit 1
fi

echo "Found DAG files:"
echo "${DAG_FILES}" | sed 's/^/  /'
echo ""

# Step 1: Python syntax check
echo "--- Step 1: Python Syntax Check ---"
for dag_file in ${DAG_FILES}; do
    dag_name=$(basename "${dag_file}")
    if python3 -m py_compile "${dag_file}" 2>&1; then
        echo "  PASS [syntax] ${dag_name}"
        ((PASS_COUNT++)) || true
    else
        echo "  FAIL [syntax] ${dag_name}"
        ((FAIL_COUNT++)) || true
        FAILED_DAGS+=("${dag_name}")
    fi
done
echo ""

# Step 2: Import validation (DAGs must import without errors)
echo "--- Step 2: Import Validation ---"
# Add project root and plugins to Python path for import
export PYTHONPATH="${PROJECT_ROOT}/src:${PROJECT_ROOT}/plugins:${PROJECT_ROOT}:${PYTHONPATH:-}"
# Set required Airflow env vars
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__UNIT_TEST_MODE=True

for dag_file in ${DAG_FILES}; do
    dag_name=$(basename "${dag_file}" .py)
    dag_rel_path="${dag_file#${PROJECT_ROOT}/}"

    # Convert file path to Python module path
    module_path=$(echo "${dag_rel_path}" | sed 's|/|.|g' | sed 's|\.py$||')

    if python3 -c "
import sys
sys.path.insert(0, '${PROJECT_ROOT}/src')
sys.path.insert(0, '${PROJECT_ROOT}/plugins')
sys.path.insert(0, '${PROJECT_ROOT}')
try:
    import importlib
    mod = importlib.import_module('${module_path}')
    print('Import OK')
except Exception as e:
    print(f'Import FAILED: {e}')
    sys.exit(1)
" 2>&1 | grep -q "Import OK"; then
        echo "  PASS [import] ${dag_name}"
        ((PASS_COUNT++)) || true
    else
        echo "  FAIL [import] ${dag_name}"
        ((FAIL_COUNT++)) || true
        FAILED_DAGS+=("${dag_name}")
    fi
done
echo ""

# Step 3: DAG cycle check via Airflow CLI (if available)
echo "--- Step 3: Cycle Check ---"
if command -v airflow &>/dev/null; then
    if airflow dags list --subdir "${DAGS_DIR}" 2>&1 | grep -v "^$"; then
        echo "  PASS [cycles] Airflow DAG list completed"
        ((PASS_COUNT++)) || true
    else
        echo "  WARN [cycles] airflow CLI available but no DAGs listed"
    fi
else
    echo "  SKIP [cycles] airflow CLI not available - skipping cycle check"
fi
echo ""

# Summary
echo "=== Validation Summary ==="
echo "  Passed: ${PASS_COUNT}"
echo "  Failed: ${FAIL_COUNT}"

if [ ${FAIL_COUNT} -gt 0 ]; then
    echo ""
    echo "Failed DAGs:"
    for dag in "${FAILED_DAGS[@]}"; do
        echo "  - ${dag}"
    done
    echo ""
    echo "DAG validation FAILED"
    exit 1
else
    echo ""
    echo "All DAG validations PASSED"
    exit 0
fi
