# 从每日涨停数据中获取每个交易日需要关注的股票代码列表

import pandas as pd

# ========== 配置 ==========
LIMIT_STA_FILE = "csv/limit_all_sta.csv"
LIMIT_ALL_FILE = "csv/limit_all.csv"
OUTPUT_FILE = "csv/import_full_list.csv"
LOOKBACK_DAYS = 3  # 向前看3天
LOOKAHEAD_DAYS = 4  # 向后看3天（含当天）
# ==========================

# 读取每日涨停数据和交易日数据
sta_df = pd.read_csv(LIMIT_STA_FILE)
all_df = pd.read_csv(LIMIT_ALL_FILE)

# 获取所有交易日, 按升序排序
trading_dates = sorted(all_df["trade_date"].astype(str).unique())

# 解析up_trade_dates字符串为列表
sta_df["up_trade_dates"] = sta_df["up_trade_dates"].apply(
    lambda x: eval(x) if isinstance(x, str) else x
)

# 快速查找用：交易日期到索引的映射
date_index = {d: i for i, d in enumerate(trading_dates)}

# 结果表初始化
result = pd.DataFrame({"date": trading_dates})

# 主逻辑：为每个交易日筛选5个交易日内的股票代码
exchange_code_list = []
for target_date in trading_dates:
    idx = date_index.get(target_date, -1)
    if idx == -1:
        exchange_code_list.append([])
        continue

    # 获取目标日期前后LOOKBACK/LOOKAHEAD范围内的有效交易日
    valid_dates = set(trading_dates[max(0, idx - LOOKBACK_DAYS) : idx + LOOKAHEAD_DAYS])

    # 找到该日期对应的ts_code列表
    codes = [
        str(row["ts_code"]).zfill(6)
        for _, row in sta_df.iterrows()
        if set(map(str, row["up_trade_dates"])) & valid_dates
    ]
    exchange_code_list.append(codes)

# 添加结果列
result["exchange_code_list"] = exchange_code_list
result["num"] = result["exchange_code_list"].apply(len)

# 调整列顺序
result = result[["date", "num", "exchange_code_list"]]

# 保存结果
result.to_csv(OUTPUT_FILE, index=False)

# 打印预览
print(result.head())
