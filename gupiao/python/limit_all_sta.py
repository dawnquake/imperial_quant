# 分析涨停数据并获取每个股票的涨停日期数据

import pandas as pd

# 读取CSV文件
df = pd.read_csv("csv/limit_all.csv")

# 排除ts_code以8和688开头的记录
df = df[~(df["ts_code"].str.startswith("8") | df["ts_code"].str.startswith("68"))]

# 确保数据类型正确
df["trade_date"] = df["trade_date"].astype(str)  # 保留yyyymmdd格式
df["ts_code"] = df["ts_code"].astype(str)
df["limit"] = df["limit"].astype(str)
# 处理limit_times为空的情况, 填充为0
df["limit_times"] = df["limit_times"].fillna(0).astype(int)

# 初始化统计DataFrame, 以不重复的ts_code为索引
result = pd.DataFrame(
    index=df["ts_code"].unique(),
    columns=[
        "ts_code",
        "up_count",
        "break_count",
        "down_count",
        "max_limit_times",
        "up_trade_dates",
    ],
)

# 计算每只股票的统计数据
for ts_code in result.index:
    stock_data = df[df["ts_code"] == ts_code]

    # 累计涨停(U)、炸板(Z)、跌停(D)次数
    result.loc[ts_code, "up_count"] = len(stock_data[stock_data["limit"] == "U"])
    result.loc[ts_code, "break_count"] = len(stock_data[stock_data["limit"] == "Z"])
    result.loc[ts_code, "down_count"] = len(stock_data[stock_data["limit"] == "D"])

    # 最高连板数量
    result.loc[ts_code, "max_limit_times"] = stock_data["limit_times"].max()

    # 只记录涨停(U)的交易日期, 保留yyyymmdd格式
    result.loc[ts_code, "up_trade_dates"] = stock_data[stock_data["limit"] == "U"][
        "trade_date"
    ].tolist()

    # 格式化ts_code, 只保留6个数字
    result.loc[ts_code, "ts_code"] = ts_code.split(".")[0]

# 重置索引并调整列顺序
result = result.reset_index(drop=True)[
    [
        "ts_code",
        "up_count",
        "break_count",
        "down_count",
        "max_limit_times",
        "up_trade_dates",
    ]
]

# 按累计涨停次数（up_count）倒序排序
result = result.sort_values(by="up_count", ascending=False)

# 保存结果到CSV
result.to_csv("csv/limit_all_sta.csv", index=False)

# 打印前几行结果
print(result.head())
