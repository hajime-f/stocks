"""
データを初期化するためのスクリプト
"""
import sys
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


def fetch_stock_data(code):
    """
    銘柄コードから株価データを取得する
    """
    url = 'https://www.nikkei.com/nkd/company/?scode=' + str(code)
    req = urllib.request.Request(url, method='GET')

    try:
        with urllib.request.urlopen(req) as res:
            content = res.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        sys.exit('\033[31m' + str(e) + '\033[0m')
    except Exception as e:
        sys.exit('\033[31m' + str(e) + '\033[0m')

    soup = BeautifulSoup(content, 'html.parser')
    values = soup.find_all("span", class_='m-stockInfo_detail_value')

    try:
        open_price = float(values[0].text.translate(
            str.maketrans({'円': '', ',': ''})))
        highest_price = float(values[1].text.translate(
            str.maketrans({'円': '', ',': ''})))
        lowest_price = float(values[2].text.translate(
            str.maketrans({'円': '', ',': ''})))
        close_price = float(soup.find(
            "dd", class_='m-stockPriceElm_value now').text.translate(str.maketrans({'円': '', ',': ''})))
        volume = int(values[3].text.translate(
            str.maketrans({'株': '', ',': ''})))
    except IndexError:
        print(f'銘柄コード{code}のデータを取得できませんでした。')

    date = soup.find("div", class_='m-stockInfo_date').text

    return open_price, highest_price, lowest_price, close_price, volume, date


if __name__ == '__main__':

    # 銘柄コードのリストを取得する
    code_list = fetch_code_list()

    for code in code_list:

        # 株価データを取得する
        open_price, highest_price, lowest_price, close_price, volume, date = fetch_stock_data(
            code)

        # 株価データをCSVファイルに保存する
        with open(f'csv/{code}.csv', 'a') as f:
            f.write(
                f'{date}, {open_price}, {highest_price}, {lowest_price}, {close_price}, {volume}\n')
