# Ad Data Pipeline - Claude Code Instructions

## Project Overview
Apache Airflow 2.8 data pipeline for Facebook ad system analytics.
Processes S3 event data for segmentation, ranking, and reporting.

## Tech Stack
- Python 3.11, Airflow 2.8.1
- PySpark for data transformations
- Pydantic for models and settings
- boto3 for AWS S3 operations
- pytest for testing

## Project Structure
- `dags/` - Airflow DAG definitions
  - `s3_management/` - S3 partitioning, tiering, compaction
  - `analytics/` - User segmentation, ranking data, reports
- `plugins/` - Custom operators, sensors, hooks
- `src/ad_data_pipeline/` - Core library
  - `config/` - Pydantic settings
  - `models/` - Data models
  - `transformations/` - Business logic
  - `utils/` - S3, Spark, metrics helpers
- `tests/` - Unit and integration tests

## Commands
- Install: `make install`
- Test: `make test` or `pytest tests/`
- Lint: `make lint` (flake8 + black + isort)
- Type check: `make type-check` (mypy)
- Format: `make format`
- Validate DAGs: `./scripts/validate-dags.sh`

## DAG Conventions
- Use Airflow 2.x TaskFlow API (@task decorator) for Python tasks
- Use classic operators for external operations (SparkSubmitOperator, etc.)
- All DAGs must have: owner, retries, retry_delay, on_failure_callback
- DAG IDs follow: `{domain}_{action}_{frequency}` pattern
- Sensors use `mode="reschedule"` to free worker slots

## Do NOT
- Do not modify DAG schedule intervals without data team review
- Do not change S3 bucket paths without updating downstream consumers
- Do not add PySpark dependencies to DAG files directly (use src/ library)
- Do not use `provide_context=True` (deprecated in Airflow 2.x)

## Data Flow
- Input: S3 `s3://fb-ad-events/{ssp,dsp}/year=/month=/day=/hour=/`
- Output: S3 `s3://fb-ad-analytics/{segments,ranking,reports}/`
- Downstream: ad-dsp-server reads ranking data, ad-ml reads segment data
