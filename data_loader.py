import pandas as pd
import os
import ta

def load_market_data(symbol: str, year: int, interval: str, start_time: str, end_time: str, add_indicators: bool = True) -> pd.DataFrame:
    """
    加载本地期货主力连续数据，适用于任意年份文件夹。

    参数：
    ----------
    symbol : str
        品种代码（如 'AG9999.XSGE'）
    year : int
        年份（如 2024, 2025）
    interval : str
        时间周期（'1min', '3min' 等）
    start_time : str
        起始日期（'YYYY-MM-DD'）
    end_time : str
        结束日期（'YYYY-MM-DD'）
    add_indicators : bool
        是否添加技术指标

    返回：
    ----------
    pd.DataFrame，index 为 datetime，含行情和技术指标字段
    """

    # 判断文件夹名（2025可能是中文括号）
    if year == 2025:
        folder_name = "2025主力连续（1-3月）_1min"
    else:
        folder_name = f"{year}主力连续_1min"

    # 拼接完整路径
    base_path = "./data/OneDrive_1_14-05-2025/qihuo_zhulilianxu_1min"
    file_path = os.path.join(base_path, folder_name, f"{symbol}_{year}_1min.csv")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"❌ 文件未找到: {file_path}")

    # 加载数据
    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
    df.index.name = "timestamp"
    df = df.sort_index()
    df = df[(df.index >= start_time) & (df.index <= end_time)]


    if 'paused' in df.columns:
        df = df[df['paused'] == 0]
    df = df.drop(columns=['paused', 'factor', 'contract'], errors='ignore')

    if interval != '1min':
        df = df.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'money': 'sum',
            'avg': 'mean',
            'high_limit': 'last',
            'low_limit': 'last',
            'pre_close': 'last',
            'open_interest': 'last'
        }).dropna()

    if add_indicators and len(df) > 50:
        df['ma_fast'] = df['close'].rolling(window=5).mean()
        df['ma_slow'] = df['close'].rolling(window=20).mean()
        df['rsi'] = ta.momentum.RSIIndicator(df['close'], window=14).rsi()
        macd = ta.trend.MACD(df['close'])
        df['macd'] = macd.macd()
        df['macd_signal'] = macd.macd_signal()
        df['macd_diff'] = df['macd'] - df['macd_signal']
        adx = ta.trend.ADXIndicator(df['high'], df['low'], df['close'])
        df['adx'] = adx.adx()
        df['di_pos'] = adx.adx_pos()
        df['di_neg'] = adx.adx_neg()
        boll = ta.volatility.BollingerBands(df['close'], window=20)
        df['boll_upper'] = boll.bollinger_hband()
        df['boll_lower'] = boll.bollinger_lband()
        df['boll_width'] = df['boll_upper'] - df['boll_lower']
        df['atr'] = ta.volatility.AverageTrueRange(df['high'], df['low'], df['close']).average_true_range()
        df['volatility'] = df['close'].pct_change().rolling(window=10).std()
        df['volume_sma'] = df['volume'].rolling(window=20).mean()
        df['volume_ratio'] = df['volume'] / df['volume_sma']
        df['price_return'] = df['close'].pct_change()
        df['momentum_10'] = df['close'].diff(periods=10)
        df['price_vs_ma'] = df['close'] - df['ma_slow']
        df['price_vs_boll'] = df['close'] - (df['boll_upper'] + df['boll_lower']) / 2

    return df


import numpy as np

def preprocess_futures_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    清洗期货分钟数据，去除异常值、前视偏差、缺失值等问题，并添加基础衍生指标。
    同时输出每一步检测到的问题及所采取的处理措施。
    """
    print("🧼 开始预处理数据...")

    original_len = len(df)

    # 1. Remove paused rows
    if 'paused' in df.columns:
        paused_count = (df['paused'] == 1.0).sum()
        if paused_count > 0:
            print(f"⚠️ 发现 {paused_count} 行交易暂停（paused == 1.0），已剔除。")
        df = df[df['paused'] == 0]

    # 2. Remove rows with zero volume
    zero_volume_count = (df['volume'] == 0).sum()
    if zero_volume_count > 0:
        print(f"⚠️ 发现 {zero_volume_count} 行成交量为 0，已剔除。")
    df = df[df['volume'] > 0]

    # 3. Drop rows with any NaNs
    nan_rows = df.isna().any(axis=1).sum()
    if nan_rows > 0:
        print(f"⚠️ 发现 {nan_rows} 行含有缺失值，已删除。")
    df = df.dropna()

    # 4. Detect and clip extreme log returns
    df['log_return'] = np.log(df['close'] / df['close'].shift(1))
    spike_count = (df['log_return'].abs() > 0.2).sum()
    if spike_count > 0:
        print(f"⚠️ 发现 {spike_count} 行价格波动过大（log_return 超过 ±20%），已裁剪。")
    df['log_return'] = df['log_return'].clip(-0.2, 0.2)

    # 5. Remove extreme rows entirely
    df = df[df['log_return'].abs() < 0.2]

    # 6. Add derived features
    df['return'] = df['close'].pct_change()
    df['momentum_10'] = df['close'].diff(10)
    df['volatility_10'] = df['return'].rolling(window=10).std()

    # 7. Volume normalization
    df['volume_sma'] = df['volume'].rolling(20).mean()
    df['volume_ratio'] = df['volume'] / (df['volume_sma'] + 1e-6)
    df['volume_ratio'] = df['volume_ratio'].clip(0, 5)

    # 8. Open interest normalization
    if 'open_interest' in df.columns:
        df['oi_change'] = df['open_interest'].diff()
        df['oi_change_norm'] = df['oi_change'] / (df['open_interest'].rolling(20).mean() + 1e-6)

    # 9. Shift to prevent lookahead bias
    shifted_cols = ['return', 'log_return', 'momentum_10', 'volatility_10', 'volume_ratio', 'oi_change_norm']
    for col in shifted_cols:
        if col in df.columns:
            df[col] = df[col].shift(1)

    # 10. Final cleanup
    final_nan_rows = df.isna().any(axis=1).sum()
    if final_nan_rows > 0:
        print(f"⚠️ 最后处理阶段发现 {final_nan_rows} 行因衍生列偏移产生 NaN，已删除。")
    df = df.dropna()

    print(f"✅ 预处理完成：原始 {original_len} 行 → 剩余 {len(df)} 行（清洗掉 {original_len - len(df)} 行）\n")

    return df


def resample_to_3min(df_1min: pd.DataFrame) -> pd.DataFrame:
    """
    将期货1分钟数据重采样为3分钟数据。

    OHLC 使用标准金融聚合方式：
    - open: 第一个值
    - high: 最大值
    - low: 最小值
    - close: 最后一个值
    成交量、成交额做加和，持仓量取最后值

    参数：
    ----------
    df_1min : pd.DataFrame
        必须包含 'open', 'high', 'low', 'close', 'volume', 'money', 'open_interest'

    返回：
    ----------
    df_3min : pd.DataFrame
    """
    if not isinstance(df_1min.index, pd.DatetimeIndex):
        raise ValueError("DataFrame must have a DatetimeIndex.")

    df_3min = df_1min.resample("3T").agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
        'money': 'sum',
        'avg': 'mean' if 'avg' in df_1min.columns else 'mean',
        'high_limit': 'last',
        'low_limit': 'last',
        'pre_close': 'last',
        'open_interest': 'last'
    })

    df_3min = df_3min.dropna(subset=['open', 'close'])  # drop incomplete intervals

    return df_3min


import matplotlib.pyplot as plt
import numpy as np

def visualize_market_data(data_dict: dict, max_symbols: int = 6, save_path: str = "./data/market_data_summary.pdf"):
    """
    可视化已加载的期货数据，展示价格与成交量，并保存为 PDF。

    参数：
    ----------
    data_dict : dict
        {symbol: DataFrame} 结构
    max_symbols : int
        最多展示多少个品种（避免图太多）
    save_path : str
        PDF 保存路径
    """
    symbols = list(data_dict.keys())[:max_symbols]
    num_plots = len(symbols)

    if num_plots == 0:
        print("⚠️ 没有可视化的数据")
        return

    fig, axes = plt.subplots(num_plots, 2, figsize=(12, 4 * num_plots), sharex=False)
    if num_plots == 1:
        axes = [axes]  # ensure iterable if only 1 row
    elif isinstance(axes[0], plt.Axes):
        axes = [axes]  # force to list of pairs

    for i, symbol in enumerate(symbols):
        df = data_dict[symbol]
        if df.empty:
            continue

        # ✅ Convert to numpy arrays to avoid pandas indexing issues
        time = np.array(df.index)
        price = np.array(df['close'])
        volume = np.array(df['volume'])

        ax_price = axes[i][0]
        ax_volume = axes[i][1]

        ax_price.plot(time, price, label='Close Price', color='blue')
        ax_price.set_title(f"{symbol} - Close Price")
        ax_price.set_ylabel("Price")
        ax_price.grid(True)

        ax_volume.bar(time, volume, width=0.01, color='gray')
        ax_volume.set_title(f"{symbol} - Volume")
        ax_volume.set_ylabel("Volume")
        ax_volume.grid(True)

    plt.tight_layout()
    plt.savefig(save_path)
    print(f"\n📄 图表已保存为：{save_path}")
    plt.close()



# ========== 批量测试代码（仅在直接运行时执行 /usr/bin/python3.10 data_loader.py） ==========
if __name__ == "__main__":
    base_dir = "./data/OneDrive_1_14-05-2025/qihuo_zhulilianxu_1min"

    # 🔧 Set your desired year range here
    start_year = 2024
    end_year = 2025

    def iterate_all_symbols():
        for year_folder in sorted(os.listdir(base_dir)):
            folder_path = os.path.join(base_dir, year_folder)
            if not os.path.isdir(folder_path):
                continue
            try:
                year = int(year_folder[:4])  # e.g., '2024主力连续_1min'
            except ValueError:
                continue  # skip invalid folder names

            # ✅ Filter only within the selected year range
            if year < start_year or year > end_year:
                continue

            for filename in os.listdir(folder_path):
                if filename.endswith(".csv"):
                    symbol = filename.replace(f"_{year}_1min.csv", "")
                    yield symbol, year

    print(f"🚀 开始加载期货数据（{start_year}–{end_year}）年区间：\n")
    success_count = 0
    fail_count = 0
    
    loaded_data = {}
    for symbol, year in iterate_all_symbols():
        try:
            df = load_market_data(
                symbol=symbol,
                year=year,
                interval="3min",
                start_time=f"{year}-01-01",
                end_time=f"{year}-12-31",
                add_indicators=False
            )
            df = preprocess_futures_data(df)
            df = resample_to_3min(df) # from 1 min to 3 min
            
            print(f"✅ {symbol} ({year}) 加载成功，共 {len(df)} 行")
            if not df.empty:
                loaded_data[f"{symbol}_{year}"] = df
            success_count += 1
        except Exception as e:
            print(f"❌ {symbol} ({year}) 加载失败：{e}")
            fail_count += 1
    
    print(f"\n✅ 成功加载 {success_count} 个文件，❌ 失败 {fail_count} 个文件。")

    # 最终可视化（最多展示前6个品种）
    visualize_market_data(loaded_data)


