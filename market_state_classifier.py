import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import os
from ta.trend import ADXIndicator
from data_loader import load_market_data, preprocess_futures_data, resample_to_3min


def label_intraday_sessions(df: pd.DataFrame, threshold: float = 1.0) -> pd.DataFrame:
    df = df.copy()
    df['state'] = 'unlabeled'

    sessions = {
        'morning': ('09:00', '11:30'),
        'afternoon': ('13:30', '15:00'),
        'night': ('21:00', '23:59')
    }

    labeled_sessions = []

    for date, group in df.groupby(df.index.date):
        for session_name, (start_t, end_t) in sessions.items():
            session_df = group.between_time(start_t, end_t)
            if len(session_df) < 5:
                continue

            start_price = session_df['close'].iloc[0]
            end_price = session_df['close'].iloc[-1]
            net_move = end_price - start_price
            volatility = session_df['close'].pct_change().std() * (len(session_df) ** 0.5)

            trend_score = net_move / (volatility * start_price + 1e-6)
            label = 'trend' if abs(trend_score) > threshold else 'range'

            session_labeled = session_df.copy()
            session_labeled['state'] = label
            session_labeled['session'] = session_name
            session_labeled['session_date'] = pd.to_datetime(date)
            labeled_sessions.append(session_labeled)

    result = pd.concat(labeled_sessions).sort_index()
    return result


def prepare_training_data(all_df: pd.DataFrame, history_slots: int = 29) -> pd.DataFrame:
    """
    准备训练数据：使用前 29 个 session 的数据预测下一个 session 的状态。
    每个样本的输入是 29 个 session 的 aggregated features，输出是接下来 session 的 label。
    """
    session_groups = all_df.groupby(['session_date', 'session'])
    features = []

    for (session_date, session_name), session_df in session_groups:
        f = {
            'session_date': session_date,
            'session': session_name,
            'close_mean': session_df['close'].mean(),
            'close_std': session_df['close'].std(),
            'volume_sum': session_df['volume'].sum(),
            'state': session_df['state'].iloc[0]  # all rows share same label
        }
        features.append(f)

    feature_df = pd.DataFrame(features).sort_values(by=['session_date', 'session']).reset_index(drop=True)

    X, y = [], []
    for i in range(history_slots, len(feature_df)):
        past = feature_df.iloc[i-history_slots:i]
        if past['state'].isnull().any():
            continue
        future_state = feature_df.iloc[i]['state']
        input_vector = past[['close_mean', 'close_std', 'volume_sum']].values.flatten()
        X.append(input_vector)
        y.append(future_state)

    X_df = pd.DataFrame(X)
    y_df = pd.Series(y, name='target')
    training_data = pd.concat([X_df, y_df], axis=1)
    return training_data


def visualize_trend_range(df: pd.DataFrame, symbol: str, save_path: str = None):
    fig, ax = plt.subplots(figsize=(14, 6))
    colors = {'trend': 'red', 'range': 'green'}
    segment_start = None

    for i in range(1, len(df)):
        state = df['state'].iloc[i]
        prev_state = df['state'].iloc[i - 1]

        if segment_start is None:
            segment_start = i - 1

        if state != prev_state or i == len(df) - 1:
            segment = df.iloc[segment_start:i]
            time = segment.index.to_numpy()
            price = segment['close'].to_numpy()
            ax.scatter(
                time,
                price,
                color=colors.get(prev_state, 'gray'),
                s=10,
                label=prev_state if prev_state not in ax.get_legend_handles_labels()[1] else None
            )
            segment_start = i

    ax.set_title(f"{symbol} - Market State (3 Sessions per Day)")
    ax.set_ylabel("Close Price")
    ax.set_xlabel("Time")
    ax.grid(True)
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))

    for label in ax.get_xticklabels():
        label.set_rotation(45)
        label.set_fontsize(8)

    ax.legend(title="Market State")
    plt.tight_layout()

    if save_path:
        plt.savefig(save_path)
        print(f"\n📄 图表已保存：{save_path}")
    else:
        plt.show()

    plt.close()


if __name__ == "__main__":
    base_dir = "./data/OneDrive_1_14-05-2025/qihuo_zhulilianxu_1min"
    all_data = []

    start_year = 2024
    end_year = 2025

    for year_folder in sorted(os.listdir(base_dir)):
        folder_path = os.path.join(base_dir, year_folder)
        if not os.path.isdir(folder_path):
            continue
        try:
            year = int(year_folder[:4])
        except ValueError:
            continue

        # ✅ Filter by year range
        if year < start_year or year > end_year:
            continue

        for filename in os.listdir(folder_path):
            if filename.endswith(".csv"):
                symbol = filename.replace(f"_{year}_1min.csv", "")
                try:
                    df = load_market_data(
                        symbol=symbol,
                        year=year,
                        interval="1min",
                        start_time=f"{year}-01-01",
                        end_time=f"{year}-12-31",
                        add_indicators=False
                    )
                    df = preprocess_futures_data(df)
                    df = resample_to_3min(df)
                    df = label_intraday_sessions(df, threshold=1.0)
                    all_data.append(df)
                    print(f"✅ {symbol} ({year}) 完成标注，共 {len(df)} 行")
                except Exception as e:
                    print(f"❌ {symbol} ({year}) 加载失败：{e}")

    if all_data:
        combined_df = pd.concat(all_data).sort_index()
        training_data = prepare_training_data(combined_df, history_slots=29)

        print("\n✅ 训练数据准备完成，共样本数：", len(training_data))
        training_data.to_csv("./data/session_training_data.csv", index=False)
        print("📄 已保存至 ./data/session_training_data.csv")
    else:
        print("⚠️ 没有任何数据被加载。请检查路径或年份设置。")


