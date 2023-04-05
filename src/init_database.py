"""
データベースを初期化するためのスクリプト
"""
import sqlite3
import urllib.request
from datetime import datetime as dt

import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


def fetch_stocks_dataframe():
    """
    ティッカーシンボルと市場のデータフレームを取得する
    """
    url = 'https://search.sbisec.co.jp/v2/popwin/info/stock/pop6040_usequity_list.html'
    with urllib.request.urlopen(url) as res:
        soup = BeautifulSoup(res, 'html.parser')

    tk_list = [t.text for t in soup.find_all("th", class_='vaM alC')]
    tk_list.pop(0)

    mk_list = [m.text for m in soup.find_all("td", class_='vaM alC')]
    mk_list.pop(0)

    df = pd.DataFrame({'ticker': tk_list, 'market': mk_list})

    return df


def fetch_stocks_values(ticker):
    """
    株価のデータフレームを取得する
    """
    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    for i in range(1, 11):

        url = f'https://us.kabutan.jp/stocks/{ticker}/historical_prices/daily?page={i}'

        try:

            with urllib.request.urlopen(url) as res:
                soup = BeautifulSoup(res, 'html.parser')

            # 日付を取得
            v_date = soup.find_all(
                "td",
                class_='py-2px font-normal text-center border border-gray-400'
            )

            v_date_list = [
                dt.strftime(dt.strptime(v.text, "%y/%m/%d"), "%Y-%m-%d")
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
            break

        df = df.sort_values('date')

    df = df.drop_duplicates()
    df = df.reset_index(drop=True)

    return df


if __name__ == '__main__':

    # ティッカーシンボルと市場のデータフレームを取得
    stocks_df = fetch_stocks_dataframe()

    # データベースに保存
    conn = sqlite3.connect('stocks.db')
    with conn:
        stocks_df.to_sql('Symbols', conn, if_exists='replace', index=False)

    # プログレスバーを定義
    bar = tqdm(total=len(stocks_df), dynamic_ncols=True,
               iterable=True, leave=False)
    bar.set_description('データを取得しています')

    for ticker in stocks_df['ticker']:

        # 株価のデータフレームを取得
        values_df = fetch_stocks_values(ticker)

        # データベースに保存
        conn = sqlite3.connect('stocks.db')
        with conn:
            values_df.to_sql(ticker, conn, if_exists='replace', index=False)

        bar.update(1)
