# DAG Development Guide

## Creating a New DAG

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

dag = DAG(
    'my_dag',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 1, 1)},
    schedule_interval='@daily'
)

def my_task(**context):
    # Use context['ti'].xcom_push() for data sharing
    pass

task = PythonOperator(
    task_id='my_task',
    python_callable=my_task,
    provide_context=True,  # Required for accessing context
    dag=dag,
)
```

## Best Practices
- Always set `provide_context=True` for Python operators
- Use `schedule_interval` string for scheduling
- Import from `airflow.operators.python_operator`
