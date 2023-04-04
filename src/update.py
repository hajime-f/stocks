"""
データを更新するためのスクリプト
"""
import os
import urllib.request

from bs4 import BeautifulSoup


def fetch_code_list():
    """
    銘柄コードのリストを取得する
    """
    page = 0
    code_list = []

    while True:

        url = f'https://portal.morningstarjp.com/StockInfo/sec/list?page={page}'
        with urllib.request.urlopen(url) as res:
            soup = BeautifulSoup(res, 'html.parser')

        codes = soup.select('td.tac')
        if len(codes) == 0:
            break
        for i, code in enumerate(codes):
            if i % 2 == 0:
                code_list.append(int(code.text))

        page += 1

    return code_list


def fetch_stock_data_from_nikkei(code):
    """
    株価データを nikkei.com から取得する
    """
    url = f'https://www.nikkei.com/nkd/company/?scode={code}'
    try:
        with urllib.request.urlopen(url) as res:
            soup = BeautifulSoup(res, 'html.parser')
    except urllib.error.HTTPError:
        return {}

    data = {}
    values = soup.find_all("span", class_='m-stockInfo_detail_value')

    try:
        data['open_price'] = float(values[0].text.translate(
            str.maketrans({'円': '', ',': ''})))
        data['highest_price'] = float(values[1].text.translate(
            str.maketrans({'円': '', ',': ''})))
        data['lowest_price'] = float(values[2].text.translate(
            str.maketrans({'円': '', ',': ''})))
        data['close_price'] = float(soup.find(
            "dd", class_='m-stockPriceElm_value now').text.translate(
                str.maketrans({'円': '', ',': ''})))
        data['volume'] = int(values[3].text.translate(
            str.maketrans({'株': '', ',': ''})))
        data['date'] = soup.find("div", class_='m-stockInfo_date').text
    except IndexError:
        data['open_price'] = -1
        data['highest_price'] = -1
        data['lowest_price'] = -1
        data['close_price'] = -1
        data['volume'] = -1
        data['date'] = ''

    return data


def fetch_stock_data_from_minkabu(code):
    """
    株価データを minkabu.jp から取得する
    """
    url = f'https://minkabu.jp/stock/{code}'
    try:
        with urllib.request.urlopen(url) as res:
            soup = BeautifulSoup(res, 'html.parser')
    except urllib.error.HTTPError:
        return {}

    data = {}
    values = soup.select('div.stock_price')

    try:
        data['close_price'] = float(values[0].text.translate(
            str.maketrans({'円': '', ',': ''})))
    except ValueError:
        data['close_price'] = -1

    values = soup.find_all(
        "td", class_='ly_vamd_inner ly_colsize_9_fix fwb tar wsnw')

    try:
        data['open_price'] = float(values[1].text.translate(
            str.maketrans({'円': '', ',': ''})))
        data['highest_price'] = float(values[2].text.translate(
            str.maketrans({'円': '', ',': ''})))
        data['lowest_price'] = float(values[3].text.translate(
            str.maketrans({'円': '', ',': ''})))
        data['volume'] = int(values[9].text.translate(
            str.maketrans({'株': '', ',': ''})))
    except ValueError:
        data['open_price'] = -1
        data['highest_price'] = -1
        data['lowest_price'] = -1
        data['volume'] = -1

    return data


if __name__ == '__main__':

    # 銘柄コードのリストを取得する
    code_list = fetch_code_list()

    for code in code_list:

        # 株価データを取得する
        data_minkabu = fetch_stock_data_from_minkabu(code)
        data_nikkei = fetch_stock_data_from_nikkei(code)

        # データを検証する
        if any(data_minkabu.values()) and any(data_nikkei.values()):

            if data_minkabu['close_price'] == -1 or data_nikkei['close_price'] == -1:
                continue
            if data_minkabu['close_price'] != data_nikkei['close_price']:
                print(f'close_price is different: {code}')
                continue

            open_price = data_nikkei['open_price']
            highest_price = data_nikkei['highest_price']
            lowest_price = data_nikkei['lowest_price']
            close_price = data_nikkei['close_price']
            volume = data_nikkei['volume']
            date = data_nikkei['date']

            # 株価データをCSVファイルに保存する
            with open(os.getcwd() + f'/csv/{code}.csv', 'a', encoding="utf-8") as f:
                f.write(
                    f'{date}, {open_price}, {highest_price}, {lowest_price}, {close_price}, {volume}\n')
