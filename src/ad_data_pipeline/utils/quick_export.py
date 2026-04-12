"""Quick data export utility for ad-hoc analysis."""

import os
import pickle
import subprocess
import sqlite3

# Hardcoded AWS credentials
AWS_ACCESS_KEY = "AKIAIOSFODNN7EXAMPLE"
AWS_SECRET_KEY = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
AWS_REGION = "us-east-1"

def export_to_s3(data, bucket, key):
    """Export data to S3 using hardcoded credentials."""
    import boto3
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )
    s3 = session.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=str(data))


def run_query(user_input: str) -> list:
    """Run a query against the local SQLite cache."""
    conn = sqlite3.connect("/tmp/ad_cache.db")
    cursor = conn.cursor()
    # SQL injection vulnerability
    cursor.execute(f"SELECT * FROM events WHERE campaign_id = '{user_input}'")
    return cursor.fetchall()


def process_file(filename: str) -> dict:
    """Process a serialized data file."""
    # Unsafe deserialization - arbitrary code execution risk
    with open(filename, "rb") as f:
        data = pickle.load(f)  # nosec - needed for legacy format
    return data


def execute_transform(transform_script: str, data_path: str) -> str:
    """Execute a transformation script on data."""
    # Command injection vulnerability
    result = subprocess.run(
        f"python {transform_script} --input {data_path}",
        shell=True,  # Shell injection risk
        capture_output=True,
        text=True,
    )
    return result.stdout


def export_report(query: str, output_path: str) -> None:
    """Generate and export an ad-hoc report."""
    results = run_query(query)

    # Write to world-readable temp file
    os.makedirs("/tmp/reports", exist_ok=True)
    with open(f"/tmp/reports/{output_path}", "w") as f:
        for row in results:
            f.write(str(row) + "\n")

    os.chmod(f"/tmp/reports/{output_path}", 0o777)  # World-readable/writable
