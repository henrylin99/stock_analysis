import pandas as pd
from datetime import timedelta
from decimal import Decimal
from utils.db_utils import DatabaseUtils

# 连接到 MySQL 数据库
conn, cursor = DatabaseUtils.connect_to_mysql()

def get_all_stock_codes():
    """
    获取所有股票代码
    """
    query = "SELECT ts_code FROM stock_basic;"
    cursor.execute(query)
    return [row[0] for row in cursor.fetchall()]

def get_recent_stock_data(ts_code, days=10):
    """
    获取某只股票最近N天的数据
    """
    query = f"""
    SELECT trade_date, open, high, low, close, pct_chg
    FROM stock_daily
    WHERE ts_code = %s
    ORDER BY trade_date DESC
    LIMIT {days};
    """
    cursor.execute(query, (ts_code,))
    data = cursor.fetchall()
    columns = ['trade_date', 'open', 'high', 'low', 'close', 'pct_chg']
    return pd.DataFrame(data, columns=columns)

def get_future_stock_data(ts_code, start_date, end_date):
    """
    获取某只股票未来区间的数据
    """
    query = """
    SELECT trade_date, close
    FROM stock_daily
    WHERE ts_code = %s AND trade_date > %s AND trade_date <= %s;
    """
    cursor.execute(query, (ts_code, start_date, end_date))
    data = cursor.fetchall()
    columns = ['trade_date', 'close']
    return pd.DataFrame(data, columns=columns)

def analyze_stock(ts_code):
    """
    分析某只股票最近10天是否满足条件
    """
    recent_data = get_recent_stock_data(ts_code)
    if recent_data.empty:
        return []

    # 按日期升序排序
    recent_data['trade_date'] = pd.to_datetime(recent_data['trade_date'])
    recent_data.sort_values('trade_date', inplace=True)

    result = []
    for _, row in recent_data.iterrows():
        if row['pct_chg'] > 9.8:
            high_price = row['high']
            # 修改后的阈值计算代码
            threshold_price = high_price - high_price * Decimal('3.82') / Decimal('100')

            # 获取后续5天的数据
            current_date = row['trade_date']
            future_data = get_future_stock_data(
                ts_code,
                current_date,
                current_date + timedelta(days=5)
            )

            # 检查是否有收盘价跌破阈值
            if future_data['close'].lt(threshold_price).any():
                continue
            result.append(row['trade_date'])

    return result

def main():
    # 获取所有股票代码
    stock_codes = get_all_stock_codes()

    # 存储满足条件的股票结果
    all_results = {}

    for ts_code in stock_codes:
        print(f"Analyzing stock: {ts_code}")
        matching_dates = analyze_stock(ts_code)
        if matching_dates:
            all_results[ts_code] = matching_dates

    # 输出结果
    print("满足条件的股票和日期：")
    for ts_code, dates in all_results.items():
        formatted_dates = [date.strftime("%Y-%m-%d") for date in dates]  # 格式化日期
        print(f"{ts_code}: {formatted_dates}")

if __name__ == "__main__":
    main()

# 关闭数据库连接
cursor.close()
conn.close()
