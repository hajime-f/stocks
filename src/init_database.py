"""
データベースを初期化するためのスクリプト
"""
import sqlite3
import urllib.request

import pandas as pd
from bs4 import BeautifulSoup


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
    ティッカーシンボルの株価を取得する
    """
    df = pd.DataFrame([], columns=['open', 'high', 'low', 'close', 'volume'])

    for i in range(1, 11):

        url = f'https://us.kabutan.jp/stocks/{ticker}/historical_prices/daily?page={i}'
        with urllib.request.urlopen(url) as res:
            soup = BeautifulSoup(res, 'html.parser')

        v_date = soup.find_all(
            "td", class_='py-2px font-normal text-center border border-gray-400')
        v_date_list = [v.text for v in v_date]
        date_df = pd.DataFrame({'date': v_date_list})

        values = soup.find_all(
            "td", class_='py-2px font-normal text-right border border-gray-400 pr-1')
        values_list = []
        for j in range(0, len(values), 7):
            v_open = values[j].text
            v_high = values[j + 1].text
            v_low = values[j + 2].text
            v_close = values[j + 3].text
            v_volume = values[j + 6].text
            values_list.append([
                v_open, v_high, v_low, v_close, v_volume
            ])
        values_df = pd.DataFrame(values_list, columns=[
                                 'open', 'high', 'low', 'close', 'volume'])

        df = pd.concat([pd.concat([date_df, values_df], axis=1), df], axis=0)

    df = df.sort_values('date')
    df = df.groupby('date').value_counts()

    return df


if __name__ == '__main__':

    # ティッカーシンボルと市場のデータフレームを取得
    stocks_df = fetch_stocks_dataframe()

    # データベースに保存
    conn = sqlite3.connect('stocks.db')
    with conn:
        stocks_df.to_sql('Symbols', conn, if_exists='replace', index=False)

    # 株価を取得
    for ticker in stocks_df['ticker']:
        values_df = fetch_stocks_values(ticker)
        breakpoint()
