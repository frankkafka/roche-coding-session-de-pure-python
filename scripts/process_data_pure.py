import pandas as pd
import sqlite3
from pathlib import Path

def read_data(users_path, orders_path):
    users = pd.read_csv(users_path)
    orders = pd.read_csv(orders_path)
    return users, orders

def transform(users, orders):
    merged = pd.merge(users, orders, on="user_id", how="inner")
    merged["order_category"] = merged["order_value"].apply(
        lambda x: "High" if x >= 100 else "Low"
    )
    return merged

def load_to_sqlite(df, db_path):
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        df.to_sql("user_orders", conn, if_exists="replace", index=False)

def main():
    users_path = "data/raw/users.csv"
    orders_path = "data/raw/orders.csv"
    output_db = "data/processed/analytics_v2.sqlite"

    users, orders = read_data(users_path, orders_path)
    merged = transform(users, orders)
    load_to_sqlite(merged, output_db)

    print(f"Merged {len(merged)} rows and wrote to {output_db}")

if __name__ == "__main__":
    main()
