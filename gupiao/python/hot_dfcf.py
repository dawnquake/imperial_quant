import tushare as ts
import pandas as pd
import time
from datetime import datetime

# 把 start_date 改为当天日期
start_date = "20240319"
end_date = datetime.now().strftime("%Y%m%d")
output_file = "csv/hot_dfcf.csv"

# 初始化 tushare token
ts.set_token("4da295bbdb01680fba579c82f1166f0831edf52683e410691f50d3ad")
pro = ts.pro_api()

# 获取交易日历（只要开盘日）
trade_cal = pro.trade_cal(
    exchange="SSE", start_date=start_date, end_date=end_date, is_open="1"
)
trade_dates = trade_cal["cal_date"].tolist()

# 尝试读取已有的csv（增量更新用）
try:
    all_data = pd.read_csv(
        output_file,
        dtype={"trade_date": str, "ts_code": str, "ts_name": str, "rank": int},
    )
    print(f"已读取已有文件, 共 {len(all_data)} 条记录")
    # 找到已经下载过的日期, 避免重复
    downloaded_dates = set(all_data["trade_date"].unique())
except FileNotFoundError:
    all_data = pd.DataFrame()
    downloaded_dates = set()
    print("未找到已有文件, 将从头开始下载")

# 计数器：每隔多少天保存一次
save_every = 10
counter = 0

# 遍历交易日
for trade_date in trade_dates:
    if trade_date in downloaded_dates:
        # print(f"{trade_date} 已存在, 跳过")
        continue

    try:
        df = pro.dc_hot(
            trade_date=trade_date, market="A股市场", hot_type="人气榜", is_new="Y"
        )
        if df is not None and not df.empty:
            df = df[["trade_date", "ts_code", "ts_name", "rank"]]
            all_data = pd.concat([all_data, df], ignore_index=True)
            print(f"{trade_date} 数据获取成功, 共 {len(df)} 条")
        else:
            print(f"{trade_date} 无数据返回")

    except Exception as e:
        print(f"{trade_date} 获取失败：{e}")

    counter += 1

    # 每 save_every 天保存一次文件
    if counter % save_every == 0:
        # 按 trade_date 倒序, rank 升序排序
        all_data = all_data.sort_values(
            by=["trade_date", "rank"], ascending=[False, True]
        )
        all_data.to_csv(output_file, index=False, encoding="utf-8-sig")
        print(f"已中途保存 {len(all_data)} 条数据到 {output_file}")

    # 防止请求过快（自定义时间）
    time.sleep(0.5)

# 最终保存一次（同样排序后保存）
all_data = all_data.sort_values(by=["trade_date", "rank"], ascending=[False, True])
all_data.to_csv(output_file, index=False, encoding="utf-8-sig")
print(f"全部完成, 总共 {len(all_data)} 条, 已保存到 {output_file}")
