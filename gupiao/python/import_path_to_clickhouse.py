import pandas as pd
import clickhouse_connect
from tqdm import tqdm

import_list = "csv/import_path.txt"

# ClickHouse connection settings
CLICKHOUSE_HOST = "localhost"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = "123"
CLICKHOUSE_DATABASE = "default"
CLICKHOUSE_TABLE = "stock"

# Selected column indices
selected_cols_idx = [1, 2, 3, 4, 7] + list(range(11, 57)) + [59, 60]

# Data Structure
schema = {
    "exchange_code": "string",  # FixedString(6) -> string
    "date": "int32",  # Int32
    "time": "int32",  # Int32
    "price": "int32",  # Int32
    "trade_count": "int64",  # Int64
    "cum_volume": "int64",  # Int64
    "cum_amount": "int64",  # Int64
    "high_price": "int32",  # Int32
    "low_price": "int32",  # Int32
    "open_price": "int32",  # Int32
    "close_price": "int32",  # Int32
    "sell_price1": "int32",  # Int32
    "sell_price2": "int32",  # Int32
    "sell_price3": "int32",  # Int32
    "sell_price4": "int32",  # Int32
    "sell_price5": "int32",  # Int32
    "sell_price6": "int32",  # Int32
    "sell_price7": "int32",  # Int32
    "sell_price8": "int32",  # Int32
    "sell_price9": "int32",  # Int32
    "sell_price10": "int32",  # Int32
    "sell_volume1": "int64",  # Int64
    "sell_volume2": "int64",  # Int64
    "sell_volume3": "int64",  # Int64
    "sell_volume4": "int64",  # Int64
    "sell_volume5": "int64",  # Int64
    "sell_volume6": "int64",  # Int64
    "sell_volume7": "int64",  # Int64
    "sell_volume8": "int64",  # Int64
    "sell_volume9": "int64",  # Int64
    "sell_volume10": "int64",  # Int64
    "buy_price1": "int32",  # Int32
    "buy_price2": "int32",  # Int32
    "buy_price3": "int32",  # Int32
    "buy_price4": "int32",  # Int32
    "buy_price5": "int32",  # Int32
    "buy_price6": "int32",  # Int32
    "buy_price7": "int32",  # Int32
    "buy_price8": "int32",  # Int32
    "buy_price9": "int32",  # Int32
    "buy_price10": "int32",  # Int32
    "buy_volume1": "int64",  # Int64
    "buy_volume2": "int64",  # Int64
    "buy_volume3": "int64",  # Int64
    "buy_volume4": "int64",  # Int64
    "buy_volume5": "int64",  # Int64
    "buy_volume6": "int64",  # Int64
    "buy_volume7": "int64",  # Int64
    "buy_volume8": "int64",  # Int64
    "buy_volume9": "int64",  # Int64
    "buy_volume10": "int64",  # Int64
    "total_sell_volume": "int64",  # Int64
    "total_buy_volume": "int64",  # Int64
}


def process_csv_file(file_path, client):
    """Process a single CSV file and insert into ClickHouse."""
    # Read only the selected columns with specified dtypes
    df = pd.read_csv(
        file_path,
        encoding="gbk",
        usecols=selected_cols_idx,
        header=0,
        names=list(schema.keys()),
        dtype={1: "string"},
    ).fillna(0)
    try:
        # Attempt to convert dtypes according to the schema
        df = df.astype(schema)
    except ValueError as e:
        # Check if the error is the specific one related to '自然日' in 'date' column
        if "自然日" in str(e).lower():
            tqdm.write(f"傻逼万得把表头重复了")
            # Remove rows where 'date' column contains '自然日'
            df = df[~df["date"].astype(str).str.contains("自然日")]
            df = df.astype(schema)
        else:
            raise e
    # Insert data into ClickHouse
    client.insert_df(f"{CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}", df)
    tqdm.write(f"Inserted {len(df)} rows")


# Initialize ClickHouse client
client = clickhouse_connect.get_client(
    host=CLICKHOUSE_HOST,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DATABASE,
)

# Read the text file containing CSV paths
with open(import_list, "r") as f:
    csv_paths = [line.strip() for line in f if line.strip()]

print(f"Found {len(csv_paths)} CSV files to process")

# Process each CSV file
for csv_path in tqdm(csv_paths, desc="Processing CSV files"):
    tqdm.write(f"Importing {csv_path}")
    process_csv_file(csv_path, client)
    break

client.close()
