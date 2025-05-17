# 从关注列表中获取数据文件地址

import pandas as pd
from clickhouse_driver import Client
import ast
import os
from collections import defaultdict

# ========== 配置 ==========
YEAR_PATHS = {
    "2023": r"D:\quant\data\2023",
    "2024": r"Z:\2024",
    "2025": r"D:\quant\data\2025",
}
CLICKHOUSE_CONFIG = {
    "host": "localhost",
    "database": "default",
    "user": "default",
    "password": "123",
}
IMPORT_FULL_LIST_CSV = "csv/import_full_list.csv"
IMPORT_MISSING_LIST_CSV = "csv/import_missing_list.csv"
IMPORT_PATH_TXT = "csv/import_path.txt"
# ==========================

# 连接 ClickHouse
client = Client(**CLICKHOUSE_CONFIG)

# 读取数据库中已有的 (exchange_code, date)
db_query = "SELECT DISTINCT exchange_code, date FROM stock"
db_data = client.execute(db_query)
db_df = pd.DataFrame(db_data, columns=["exchange_code", "date"])
db_df["date"] = db_df["date"].astype(int)

# 读取导入关注列表
csv_df = pd.read_csv(IMPORT_FULL_LIST_CSV)
csv_df["date"] = csv_df["date"].astype(int)

# 查找未导入的 exchange_code 列表
result_rows = []
for _, row in csv_df.iterrows():
    date = row["date"]
    codes_to_check = ast.literal_eval(row["exchange_code_list"])
    db_codes = set(db_df.loc[db_df["date"] == date, "exchange_code"])
    missing_codes = [c for c in codes_to_check if c not in db_codes]
    if missing_codes:
        result_rows.append({"date": date, "exchange_code_list": missing_codes})

result_df = pd.DataFrame(result_rows)
result_df.to_csv(IMPORT_MISSING_LIST_CSV, index=False)
print(f"已生成 {IMPORT_MISSING_LIST_CSV}")

# 构建 (year, date, exchange_code) -> [file_paths] 映射
file_map = defaultdict(list)
for year, base_path in YEAR_PATHS.items():
    if not os.path.exists(base_path):
        continue
    for date_folder in os.listdir(base_path):
        date_path = os.path.join(base_path, date_folder)
        if not os.path.isdir(date_path):
            continue
        for file_name in os.listdir(date_path):
            if file_name.endswith(".csv"):
                code = file_name[:-4]
                file_map[(year, date_folder, code)].append(
                    os.path.normpath(os.path.join(date_path, file_name))
                )
print("已扫描年份目录")

# 读取缺失导入列表, 定位对应 CSV 文件路径
import_list_df = pd.read_csv(IMPORT_MISSING_LIST_CSV)
csv_paths = []
for _, row in import_list_df.iterrows():
    date_str = str(row["date"])
    year = date_str[:4]
    codes = ast.literal_eval(row["exchange_code_list"])
    for code in codes:
        csv_paths.extend(file_map.get((year, date_str, code), []))

# 写出路径列表
with open(IMPORT_PATH_TXT, "w", encoding="utf-8") as f:
    for path in csv_paths:
        f.write(path + "\n")
print(f"已生成 {IMPORT_PATH_TXT}")
