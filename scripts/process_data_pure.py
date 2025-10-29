"""
Roche Coding Session - Pure Python Solution (Final)
--------------------------------------------------
This script demonstrates a simple ETL pipeline that:
1. Reads user and order data from CSV files
2. Performs an inner join on 'user_id'
3. Adds a derived column 'order_category' based on the order amount
4. Loads the final result into a SQLite database

Author: Arun Kumar Sathiya Moorthy
Date: 2025-10-28
"""

import pandas as pd
import sqlite3
from pathlib import Path


def read_data(users_path: str, orders_path: str):
    """
    Reads input CSVs for users and orders.

    Args:
        users_path (str): File path to users.csv
        orders_path (str): File path to orders.csv

    Returns:
        tuple: (users_df, orders_df) as pandas DataFrames
    """
    users = pd.read_csv(users_path)
    orders = pd.read_csv(orders_path)
    return users, orders


def transform(users: pd.DataFrame, orders: pd.DataFrame) -> pd.DataFrame:
    """
    Performs an inner join on 'user_id' and adds a derived column.

    Args:
        users (DataFrame): Users dataset
        orders (DataFrame): Orders dataset

    Returns:
        DataFrame: Joined and enriched dataset
    """
    # Inner join on user_id
    merged = pd.merge(users, orders, on="user_id", how="inner")

    # Add a new column 'order_category' using 'amount'
    merged["order_category"] = merged["amount"].apply(
        lambda x: "High" if x >= 100 else "Low"
    )

    return merged


def load_to_sqlite(df: pd.DataFrame, db_path: str):
    """
    Loads the DataFrame into a SQLite database.

    Args:
        df (DataFrame): Final dataset to store
        db_path (str): Path to the output SQLite database file
    """
    # Ensure the output directory exists
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # Write the DataFrame to SQLite
    with sqlite3.connect(db_path) as conn:
        df.to_sql("user_orders", conn, if_exists="replace", index=False)


def main():
    """Main ETL pipeline execution logic."""
    # Define input/output paths
    users_path = "data/raw/users.csv"
    orders_path = "data/raw/orders.csv"
    output_db = "data/processed/analytics_v2.sqlite"

    # Extract phase
    users, orders = read_data(users_path, orders_path)

    # Transform phase
    merged = transform(users, orders)

    # Load phase
    load_to_sqlite(merged, output_db)

    print(f"Merged {len(merged)} rows and wrote to {output_db}")


# Execute only if this file is run directly
if __name__ == "__main__":
    main()
