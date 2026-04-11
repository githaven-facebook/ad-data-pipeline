# Ad Data Pipeline

Airflow-based data pipeline for processing Facebook ad event data stored in S3.
Runs scheduled DAGs for S3 data management, user segmentation, ranking feature
computation, and advertiser report generation.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                       ad-event-stream (upstream)                    │
│              Produces Parquet files to S3 landing zone              │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ s3://fb-ad-raw-events/landing/
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     S3 DATA MANAGEMENT LAYER                        │
│                                                                     │
│  s3_partitioning_daily (2 AM UTC)                                   │
│    Raw landing → Hive partitions (year/month/day/hour/event_type)  │
│    s3://fb-ad-processed-events/events/                              │
│                                                                     │
│  s3_compaction_daily (4 AM UTC)                                     │
│    Small Parquet files → Optimally-sized files (256 MB target)     │
│    Deduplication on event_id                                        │
│                                                                     │
│  s3_hot_cold_tiering_weekly (Sunday 3 AM UTC)                       │
│    HOT (< 30d) → WARM (30-90d) → COLD (Glacier) → ARCHIVE (365d+) │
└─────────────────────────┬───────────────────────────────────────────┘
                          │ s3://fb-ad-processed-events/events/
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       ANALYTICS LAYER                               │
│                                                                     │
│  user_segmentation_daily (6 AM UTC)                                 │
│    Events → RFM features → Segment assignment → Redis              │
│    Segments: champions, loyal, potential_loyal, new_users,         │
│             at_risk, cant_lose, hibernating, lost                   │
│                                                                     │
│  ranking_data_hourly (every hour) ← CRITICAL PATH                  │
│    Events → CTR/CVR/eCPM (1h/6h/24h windows) → Feature vectors    │
│    → s3://fb-ad-feature-store/ + Redis cache                       │
│    SLA: 30 minutes                                                  │
│                                                                     │
│  advertiser_report_daily (7 AM UTC)                                 │
│    Previous day events → Campaign hierarchy aggregation            │
│    → KPIs (CTR, CVR, CPA, CPC, CPM, ROAS, eCPM)                   │
│    → PostgreSQL + Email notifications                               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## DAG Descriptions

### S3 Management DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `s3_partitioning_daily` | `0 2 * * *` | Repartitions raw event data into Hive-style S3 partitions |
| `s3_compaction_daily` | `0 4 * * *` | Compacts small Parquet files (< 64 MB) into 256 MB files |
| `s3_hot_cold_tiering_weekly` | `0 3 * * 0` | Moves aged partitions to S3 Glacier to reduce storage costs |

### Analytics DAGs

| DAG | Schedule | SLA | Description |
|-----|----------|-----|-------------|
| `user_segmentation_daily` | `0 6 * * *` | 8h | RFM-based user segmentation with full/incremental modes |
| `ranking_data_hourly` | `@hourly` | 30m | Ranking feature computation for real-time ad serving |
| `advertiser_report_daily` | `0 7 * * *` | 10h | Daily advertiser performance reports with email delivery |

---

## Data Flow

```
S3 Raw Events
  year=YYYY/month=MM/day=DD/ (landing zone)
        │
        ├─► Partitioning ──► year=YYYY/month=MM/day=DD/hour=HH/event_type=X/
        │                    (processed events bucket)
        │
        ├─► Compaction   ──► Merges small files, deduplicates on event_id
        │
        ├─► Tiering      ──► S3 Standard → S3-IA → Glacier IR → Glacier DA
        │
        ├─► Segmentation ──► User features → RFM scores → Redis segments
        │                    (90-day lookback, full rebuild Mondays)
        │
        ├─► Ranking      ──► CTR/CVR/eCPM at 1h/6h/24h windows
        │                    Time-decayed features → Feature Store
        │
        └─► Reports      ──► Campaign hierarchy rollup → KPIs
                             → PostgreSQL → Advertiser emails
```

---

## Configuration Reference

All settings are loaded from environment variables with the `AD_PIPELINE_` prefix
(see `src/ad_data_pipeline/config/settings.py`).

### S3 Buckets

| Variable | Default | Description |
|----------|---------|-------------|
| `AD_PIPELINE_RAW_EVENTS_BUCKET` | `fb-ad-raw-events` | Raw ad events landing zone |
| `AD_PIPELINE_PROCESSED_EVENTS_BUCKET` | `fb-ad-processed-events` | Partitioned processed events |
| `AD_PIPELINE_COLD_STORAGE_BUCKET` | `fb-ad-cold-storage` | S3 Glacier cold storage |
| `AD_PIPELINE_ANALYTICS_BUCKET` | `fb-ad-analytics` | Analytics output bucket |
| `AD_PIPELINE_FEATURE_STORE_BUCKET` | `fb-ad-feature-store` | ML feature store bucket |

### Data Retention

| Variable | Default | Description |
|----------|---------|-------------|
| `AD_PIPELINE_HOT_DATA_RETENTION_DAYS` | `30` | Days before moving to warm tier |
| `AD_PIPELINE_COLD_DATA_RETENTION_DAYS` | `365` | Days before archiving |
| `AD_PIPELINE_MAX_PARTITION_SIZE_MB` | `512` | Max Parquet file size before splitting |
| `AD_PIPELINE_MIN_PARTITION_SIZE_MB` | `64` | Min size before compaction |

### Analytics

| Variable | Default | Description |
|----------|---------|-------------|
| `AD_PIPELINE_SEGMENTATION_LOOKBACK_DAYS` | `90` | Lookback window for RFM features |
| `AD_PIPELINE_RANKING_FEATURES_LOOKBACK_HOURS` | `24` | Lookback for ranking features |
| `AD_PIPELINE_DATA_FRESHNESS_SLA_MINUTES` | `30` | Max staleness for ranking features |

---

## Development Setup

### Prerequisites

- Python 3.10+
- Docker and Docker Compose
- AWS CLI (for LocalStack testing)

### Local Installation

```bash
# Clone the repository
git clone https://github.com/fb-data/ad-data-pipeline.git
cd ad-data-pipeline

# Install all dependencies including dev tools
make install

# Verify installation
python -c "import ad_data_pipeline; print('OK')"
```

### Running Tests

```bash
# Run all unit tests with coverage
make test

# Run only unit tests (fast)
make test-unit

# Run integration tests (requires Airflow)
make test-integration

# Run all tests including slow ones
bash scripts/run-tests.sh --integration
```

### Code Quality

```bash
# Format code
make format

# Run all linters
make lint

# Type checking
make type-check

# Run pre-commit hooks on all files
make pre-commit
```

### DAG Validation

```bash
# Validate all DAG files (syntax + import + cycle check)
make validate-dags

# Or directly
bash scripts/validate-dags.sh
```

---

## Deployment Guide

### Local Development with Docker

```bash
# Build the Airflow image
make docker-build

# Start the full local stack (Airflow + Postgres + Redis + LocalStack)
make docker-up

# Access Airflow UI at http://localhost:8080
# Username: admin / Password: admin

# View logs
make docker-logs

# Stop all services
make docker-down
```

### Production Deployment (AWS MWAA)

1. **Build and push Docker image** - CI/CD pipeline handles this on merge to `main`
2. **Update MWAA environment** - `cd.yml` workflow updates the MWAA environment
3. **DAG deployment** - DAG files are synced to S3 via the CD pipeline
4. **Secrets** - Configure via AWS Secrets Manager or Airflow Variables/Connections

#### Required AWS Resources

- MWAA Environment (Airflow 2.8.x)
- S3 buckets (see Configuration Reference)
- ECR repository: `ad-data-pipeline`
- IAM role with S3/Glue/SES permissions
- Redis (ElastiCache) for segment storage
- PostgreSQL (RDS) for report storage

#### Airflow Connections Required

| Connection ID | Type | Purpose |
|---------------|------|---------|
| `aws_default` | Amazon Web Services | S3, SES access |
| `spark_default` | Apache Spark | Spark cluster submission |
| `postgres_reports` | Postgres | Advertiser report database |

---

## Monitoring and Alerting

### Metrics (StatsD)

All DAGs emit metrics via StatsD to Datadog:

- `ad_pipeline.dag.started` - DAG run started
- `ad_pipeline.dag.success` - Successful DAG completion with duration
- `ad_pipeline.dag.failed` - DAG failure counter
- `ad_pipeline.task.rows_processed` - Rows processed per task
- `ad_pipeline.task.bytes_processed` - Bytes processed per task
- `ad_pipeline.task.duration` - Task execution time (ms)

### Alerting

| Alert | Channel | Condition |
|-------|---------|-----------|
| DAG failure (any) | Slack `#data-platform-alerts` | Task fails after all retries |
| Ranking SLA miss | PagerDuty + Slack | `ranking_data_hourly` misses 30m SLA |
| Segmentation failure | Slack | `user_segmentation_daily` fails |
| Report delivery failure | Slack + Email | `advertiser_report_daily` fails |

Configure via environment variables:
```bash
AD_PIPELINE_PAGERDUTY_INTEGRATION_KEY=your-key
AD_PIPELINE_SLACK_WEBHOOK_URL=https://hooks.slack.com/...
```

---

## Data Quality Framework

Data quality checks are implemented via `DataQualityOperator` (see
`plugins/operators/data_quality_operator.py`) and run after each partition
processing step.

### Check Types

| Check | Description | Failure Action |
|-------|-------------|----------------|
| `row_count` | Minimum row count threshold | Fail task |
| `null_ratio` | Null fraction per column | Configurable warn/fail |
| `schema` | Expected columns present | Fail task |
| `outlier` | Numeric column range check | Warning |

### Compaction Safety

Before deleting original files after compaction:
- Output row count verified against input (max 5% dedup loss allowed)
- Output file size must not exceed 110% of input size
- Zero-row outputs always rejected

---

## Project Structure

```
ad-data-pipeline/
├── src/ad_data_pipeline/        # Core library (installable package)
│   ├── config/settings.py       # Pydantic-based settings
│   ├── models/                  # Pydantic data models
│   │   ├── event.py             # AdEvent, SSPEvent, DSPEvent, UserAction
│   │   ├── segment.py           # UserSegment, SegmentRule, SegmentCriteria
│   │   └── report.py            # AdvertiserReport, CampaignMetrics
│   ├── transformations/         # Pure transformation logic
│   │   ├── partitioner.py       # S3 partition management
│   │   ├── compactor.py         # Parquet compaction
│   │   ├── tiering.py           # Hot/cold classification
│   │   ├── segmentation.py      # RFM user segmentation
│   │   ├── ranking_features.py  # CTR/CVR/eCPM feature computation
│   │   └── report_builder.py    # KPI aggregation
│   └── utils/                   # Shared utilities
│       ├── s3.py                # S3 helpers with retry
│       ├── spark.py             # SparkSession factory
│       ├── metrics.py           # StatsD metrics client
│       └── alerts.py            # PagerDuty/Slack alerts
├── plugins/                     # Airflow plugins
│   ├── operators/               # Custom Airflow operators
│   ├── sensors/                 # Custom Airflow sensors
│   └── hooks/                   # Enhanced S3 hook
├── dags/                        # Airflow DAG definitions
│   ├── s3_management/           # Partitioning, compaction, tiering
│   └── analytics/               # Segmentation, ranking, reports
├── tests/
│   ├── unit/                    # Unit tests (no external deps)
│   └── integration/             # DAG structure tests
└── .github/workflows/           # CI/CD pipelines
```
