import os
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from tqdm import tqdm
import itertools
import time
import sys


def get_bq_client(cred_path: str) -> bigquery.Client:
    """Initialize and return a BigQuery client."""
    if not os.path.exists(cred_path):
        raise FileNotFoundError(f"❌ Service account file not found: {cred_path}")

    creds = service_account.Credentials.from_service_account_file(cred_path)
    client = bigquery.Client(credentials=creds, project=creds.project_id)
    print(f"✅ Connected to BigQuery project: {creds.project_id}")
    return client


def load_query(path: str) -> str:
    """Read SQL query from file."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Query file not found: {path}")
    with open(path, "r") as f:
        query = f.read()
    print(f"📄 Loaded query from: {path}")
    return query


def run_query(client: bigquery.Client, query: str) -> pd.DataFrame:
    """
    Execute a BigQuery SQL query with a live spinner and elapsed time display.
    Also prints cost and performance summary.
    """
    print("\n🚀 Submitting BigQuery job...")
    start_time = time.time()

    job = client.query(query)
    print(f"🧠 Job ID: {job.job_id}")

    # Spinner animation setup
    spinner = itertools.cycle(["|", "/", "-", "\\"])
    while not job.done():
        elapsed = int(time.time() - start_time)
        sys.stdout.write(f"\r{next(spinner)} ⏳ Query running... {elapsed}s elapsed")
        sys.stdout.flush()
        time.sleep(0.3)

    # Stop spinner
    print("\n✅ Query completed! Fetching results...")

    # Fetch DataFrame
    df = job.to_dataframe()
    end_time = time.time()
    elapsed = round(end_time - start_time, 2)

    # Get job statistics
    job.reload()
    stats = job._properties.get("statistics", {}).get("query", {})
    total_bytes = int(stats.get("totalBytesProcessed", 0))
    total_gb = total_bytes / (1024**3)
    estimated_cost = total_gb * 5.0 / 1024  # $5 per TB scanned

    print("\n📊 Job Summary:")
    print(f"   • Rows fetched: {len(df):,}")
    print(f"   • Total bytes processed: {total_gb:.3f} GB")
    print(f"   • Estimated cost: ${estimated_cost:.4f} USD")
    print(f"   • Execution time: {elapsed:.2f} seconds")
    print(f"   • Billing tier: {stats.get('billingTier', 'N/A')}")
    print("--------------------------------------------------")

    return df

def save_to_csv(df: pd.DataFrame, output_path: str):
    """Save DataFrame to CSV. Create folder if needed."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"💾 Data saved to: {output_path}")


def ingest_dataset(client, query_file, output_file):
    """Generic ingestion routine for a single dataset."""
    query = load_query(query_file)
    df = run_query(client, query)
    save_to_csv(df, output_file)
    return df


def main():
    # Common setup
    cred_path = "/Users/srdeo/OneDrive - Copart, Inc/secrets/cprtpr-datastewards-sp1-614d7e297848 (1).json"
    client = get_bq_client(cred_path)

    # Define all ingestion tasks
    tasks = [
        {
            "name": "Active buyers",
            "query_file": "src/queries/active_buyers.sql",
            "output_file": "data/raw/active_buyers.csv"
        },
        {
            "name": "Non-active buyers",
            "query_file": "src/queries/non_active_buyers.sql",
            "output_file": "data/raw/non_active_buyers.csv"
        },
        {
            "name": "Popular lots",
            "query_file": "src/queries/popular_lots.sql",
            "output_file": "data/raw/popular_lots.csv"
        },
        {
            "name": "Upcoming lots",
            "query_file": "src/queries/upcoming_lots.sql",
            "output_file": "data/raw/upcoming_lots.csv"
        },
    ]

    # Loop through all tasks
    for task in tasks:
        print(f"\n📊 Starting ingestion for: {task['name']}")
        ingest_dataset(client, task["query_file"], task["output_file"])

    print("\n✅ All datasets ingested successfully!")


if __name__ == "__main__":
    main()
