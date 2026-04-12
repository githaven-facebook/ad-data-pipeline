# Contributing

## Setup
1. Install Python 3.8
2. Create virtualenv: `pipenv install`
3. Activate: `pipenv shell`
4. Initialize Airflow DB: `airflow initdb`

## Running Locally
```bash
airflow webserver -p 8080
airflow scheduler
```

## Testing
```bash
python -m pytest tests/ -v
```

## Adding DAGs
1. Create DAG file in `dags/` directory
2. Test locally with `airflow test <dag_id> <task_id> <date>`
3. Submit PR for review

## Code Style
We use autopep8 for formatting. Run `autopep8 --in-place --recursive .`
