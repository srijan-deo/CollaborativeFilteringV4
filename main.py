import os
import pandas as pd
from google.cloud import bigquery


def run_data_ingestion():
    """
    Step 1 : This function will import data from BigQuery and save it in csv format.
    :return: CSV
    """
    print("Importing data from BigQuery...")

    cred_path = "/Users/srdeo/OneDrive - Copart, Inc/secrets/cprtpr-datastewards-sp1-614d7e297848 (1).json"

    queries = {
        "active_buyers": src/queries/active_buyers.sql,
        "non_active_buyers": src/queries/non_active_buyers.sql,
        "popular_lots": src/queries/popular_lots.sql,
        "upcoming_lots": src/queries/upcoming_lots.sql
    }

    for name, path in queries.items():
        print(f"\n Running query : {name}")
        query =