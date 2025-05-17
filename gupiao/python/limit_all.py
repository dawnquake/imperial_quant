# 获取历史每日涨停列表

import tushare as ts
import pandas as pd
from datetime import datetime
import os
import time

# ========== 配置 ==========
TS_TOKEN = "4da295bbdb01680fba579c82f1166f0831edf52683e410691f50d3ad"
OUTPUT_FILE = "csv/limit_all.csv"
START_DATE = "20230101"
END_DATE = datetime.now().strftime("%Y%m%d")
SAVE_BATCH = 10
SLEEP_TIME = 0.5
# ==========================

ts.set_token(TS_TOKEN)
pro = ts.pro_api()

# 获取所有交易日
trade_dates = pro.trade_cal(start_date=START_DATE, end_date=END_DATE, is_open="1")["cal_date"].tolist()

# 获取已爬取的日期
if os.path.exists(OUTPUT_FILE):
    existing_dates = set(pd.read_csv(OUTPUT_FILE, dtype={"trade_date": str})["trade_date"].unique())
else:
    existing_dates = set()

# 找出未爬取的日期
to_fetch = [d for d in trade_dates if d not in existing_dates]

print(f"Total missing dates: {len(to_fetch)}")

buffer = []

for i, date in enumerate(to_fetch):
    print(f"Fetching {date}")
    try:
        df = pro.limit_list_d(trade_date=date)
        if not df.empty:
            buffer.append(df)
    except Exception as e:
        print(f"Error {date}: {e}")

    if len(buffer) >= SAVE_BATCH or i == len(to_fetch) - 1:
        if buffer:
            batch_df = pd.concat(buffer, ignore_index=True)

            # 临时追加（后面会统一排序重写文件）
            mode = "a" if os.path.exists(OUTPUT_FILE) else "w"
            header = not os.path.exists(OUTPUT_FILE)
            batch_df.to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)

            print(f"Saved batch up to {date}")
            buffer = []

    time.sleep(SLEEP_TIME)

# ========== 最终全表排序 ==========
if os.path.exists(OUTPUT_FILE):
    df = pd.read_csv(OUTPUT_FILE, dtype={"trade_date": str})
    df = df.sort_values(by=["trade_date", "ts_code"], ascending=[False, True])
    df.to_csv(OUTPUT_FILE, index=False)
    print("Final sorted file saved.")

print("Update done.")
