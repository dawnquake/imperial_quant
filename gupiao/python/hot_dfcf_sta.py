# 分析东方财富热榜数据

import pandas as pd

# 配置参数
inputFile = "csv/hot_dfcf.csv"
startDate = "20230101"  # 格式为YYYYMMDD
endDate = ""
topN = 10

# 自动生成输出文件名
outputFile = f"csv/hot_dfcf_top{topN}"
if startDate:
    outputFile += f"_from{startDate}"
if endDate:
    outputFile += f"_to{endDate}"
outputFile += ".csv"

# 读取数据
df = pd.read_csv(inputFile)

# 检查必要列
requiredCols = ["trade_date", "ts_code", "ts_name", "rank"]
if not all(col in df.columns for col in requiredCols):
    missing = [col for col in requiredCols if col not in df.columns]
    print(f"缺少列: {', '.join(missing)}")
    exit()

# 日期格式处理
df["trade_date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
df = df.dropna(subset=["trade_date"])

# 日期筛选
if startDate:
    df = df[df["trade_date"] >= pd.to_datetime(startDate, format="%Y%m%d")]
if endDate:
    df = df[df["trade_date"] <= pd.to_datetime(endDate, format="%Y%m%d")]
if df.empty:
    print("筛选后无数据")
    exit()

# 筛选排名Top N
df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
df = df[df["rank"] <= topN].dropna(subset=["rank"])
if df.empty:
    print(f"没有排名前{topN}的数据")
    exit()

# 分组统计
result = (
    df.groupby(["ts_code", "ts_name"])
    .agg(
        times=("rank", "count"), dateList=("trade_date", lambda x: ", ".join(sorted({d.strftime("%Y%m%d") for d in x})))
    )
    .reset_index()
    .sort_values(by="times", ascending=False)
)

# 保存结果
result.to_csv(outputFile, index=False, encoding="utf-8-sig")
print(f"结果已保存：{outputFile}")
print(result.head())
