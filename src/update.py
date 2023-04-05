"""
データを更新するためのスクリプト
"""
import math
import sqlite3
import urllib.request
from datetime import date, datetime

import pandas as pd
from bs4 import BeautifulSoup


def fetch_symbols_dataframe():
    """
    ティッカーシンボルのデータフレームを取得する
    """
    query = 'select symbol from Symbols;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)

    return df


def obtain_num_days(symbol):
    """
    株価のページから最新の日付を取得し、データベースに格納されている最新の日付との差分を計算する
    """

    # データベースに格納されている最新の日付を取得する
    query = f'select distinct date from {symbol} order by date desc limit 1;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df = pd.read_sql_query(query, conn)
    last_date = df['date'].values[0]
    last_date_dt = datetime.strptime(last_date, '%Y-%m-%d')
    last_date_1 = last_date_dt.date()

    # 株価のページから最新の日付を取得する
    url = f'https://us.kabutan.jp/stocks/{symbol}/historical_prices/daily?page=1'
    with urllib.request.urlopen(url) as res:
        soup = BeautifulSoup(res, 'html.parser')
    v_date = soup.find_all(
        "td",
        class_='py-2px font-normal text-center border border-gray-400'
    )
    last_date = v_date[0].text
    last_date_dt = datetime.strptime(last_date, '%y/%m/%d')
    last_date_2 = last_date_dt.date()

    # 差分日数を計算する
    num_days = (last_date_2 - last_date_1).days

    return num_days


def fetch_values_dataframe(url):

    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    try:

        with urllib.request.urlopen(url) as res:
            soup = BeautifulSoup(res, 'html.parser')

        # 日付を取得
        v_date = soup.find_all(
            "td",
            class_='py-2px font-normal text-center border border-gray-400'
        )
        v_date_list = [
            datetime.strftime(
                datetime.strptime(v.text, "%y/%m/%d"),
                "%Y-%m-%d")
            for v in v_date
        ]
        v_date_df = pd.DataFrame({'date': v_date_list})

        # 株価を取得
        v_values = soup.find_all(
            "td",
            class_='py-2px font-normal text-right border border-gray-400 pr-1'
        )
        v_values_list = []
        for j in range(0, len(v_values), 7):
            v_open = v_values[j].text
            v_high = v_values[j + 1].text
            v_low = v_values[j + 2].text
            v_close = v_values[j + 3].text
            v_volume = v_values[j + 6].text
            v_values_list.append([
                v_open, v_high, v_low, v_close, v_volume
            ])
        v_values_df = pd.DataFrame(v_values_list, columns=[
            'open', 'high', 'low', 'close', 'volume'])

        # 日付と株価を結合
        df = pd.concat(
            [pd.concat([v_date_df, v_values_df], axis=1), df], axis=0
        )

    except urllib.error.HTTPError:
        pass

    return df


def fetch_stock_values(symbol):
    """
    株価のデータフレームを取得する
    """

    # 差分日数を計算する
    num_days = obtain_num_days(symbol)

    # ページネーションの数を計算する
    pagenation = math.ceil(num_days / 30)

    df = pd.DataFrame(
        [],
        columns=['date', 'open', 'high', 'low', 'close', 'volume']
    )

    for i in range(1, pagenation + 1):

        # 株価のページからデータフレームを取得する
        url = f'https://us.kabutan.jp/stocks/{symbol}/historical_prices/daily?page={i}'
        values_df = fetch_values_dataframe(url)
        df = pd.concat([df, values_df], axis=0)

    df = df.drop_duplicates()

    return df


def make_distinct(df_new, symbol):
    """
    重複行を削除する
    """
    query = f'select distinct * from {symbol} order by date desc;'
    conn = sqlite3.connect('stocks.db')
    with conn:
        df_old = pd.read_sql_query(query, conn)

    df = pd.concat([df_old, df_new], axis=0)

    df = df.sort_values('date')
    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    return df


if __name__ == '__main__':

    # ティッカーシンボルのデータフレームを取得する
    symbol_df = fetch_symbols_dataframe()

    for symbol in symbol_df['symbol']:

        # 株価のデータフレームを取得する
        values_df = fetch_stock_values(symbol)
        values_df = make_distinct(values_df, symbol)

        # データベースに格納する
        conn = sqlite3.connect('stocks.db')
        with conn:
            values_df.to_sql(symbol, conn, if_exists='replace', index=False)

        breakpoint()
