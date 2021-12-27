from pathlib import Path

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup

# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context


def get_eps(name="G36"):
    url = f"https://data.masvn.com/Controls/Report/StockFinance_Quarter.aspx?language=vi&scode={name}&page=1"
    data = pd.read_html(url)
    df = data[0]
    data_eps = df.iloc[[13, 17], [0, 2, 3, 4, 5]].reset_index(drop=True)
    data_eps.columns = data_eps.iloc[0]
    data_eps = data_eps.drop([0])

    for page in range(2, 6):
        url = f"https://data.masvn.com/Controls/Report/StockFinance_Quarter.aspx?language=vi&scode={name}&page={page}"
        data = pd.read_html(url)
        df = data[0]
        eps = df.iloc[[13, 17], [2, 3, 4, 5]].reset_index(drop=True)
        eps.columns = eps.iloc[0]
        eps = eps.drop([0])
        data_eps = pd.concat([eps, data_eps], axis=1)

    query = str(quarter) + "/"
    filter_col = [col for col in data_eps if query in col]
    select_col = ["CHỈ TIÊU CƠ BẢN"] + filter_col
    df_1 = data_eps[select_col].reset_index(drop=True)
    return df_1


def get_data(url, first=False):
    url_2 = url.replace("BSheet", "IncSta").replace("bao-cao-tai-chinh-", "ket-qua-hoat-dong-kinh-doanh-")
    url_3 = url.replace("BSheet", "CashFlow").replace("bao-cao-tai-chinh-", "luu-chuyen-tien-te-gian-tiep-")
    print(url)
    data1 = pd.read_html(url)
    # get name colums
    names = data1[2]
    cls_name = names.iloc[0].tolist()
    cls_name[0] = "Tiêu chí"
    cls_name = cls_name[:5]

    # get data from 1 to 9
    # get data
    df1 = data1[3]
    df1 = df1.iloc[[0, 13, 14, 15, 16, 17, 21], [0, 1, 2, 3, 4]]
    df1.columns = cls_name

    # lấy tài sản cố định vô hình
    # Make a GET request to fetch the raw HTML content
    html_content = requests.get(url).text

    # Parse the html content
    soup = BeautifulSoup(html_content, "lxml")

    # print(soup.prettify()) # print the parsed data of html
    table = soup.find("table", id="tableContent")

    rows = table.find_all('tr')

    tscd = []
    for row in rows:
        cols = row.find_all('td')
        cols = [x.text.strip() for x in cols]
        if len(cols) > 0 and "Tài sản cố định vô hình" in cols[0]:
            tscd = cols
    a = [int(x.replace(",", "")) if len(x) > 0 else 0 for x in tscd[1:5]]
    b = [tscd[0]]
    b = b + a
    s = pd.Series(b, index=df1.columns)
    df1 = df1.append(s, ignore_index=True)
    df1 = df1.iloc[[0, 7, 1, 2, 3, 4, 5, 6]]

    # get data from 10 to 13
    data2 = pd.read_html(url_2)
    df2 = data2[3]
    df2 = df2.iloc[[0, 2, 3, 18], [0, 1, 2, 3, 4]]
    df2.columns = cls_name

    # geta data 14 (25 gian tiep 8 truc tiep)
    data3 = pd.read_html(url_3)
    df3 = data3[3]
    df3 = df3.iloc[[25], [0, 1, 2, 3, 4]]
    df3.columns = cls_name

    df = pd.concat([df1, df2, df3], axis=0)
    df = df.replace(np.nan, 0)
    df.iloc[:, [1, 2, 3, 4]] = df.iloc[:, [1, 2, 3, 4]].astype('Int64')

    df = df.reset_index(drop=True)

    if first:
        return df
    else:
        return df.iloc[:, [1, 2, 3, 4]]


def wrap(url, name):
    print(f"Crawl head data {name}")
    df = get_data(url, True)
    for i in range(end - 1, start - 1, -1):
        url1 = url.replace("2021", str(i))
        df1 = get_data(url1)
        df = pd.concat([df1, df], axis=1)

    query = str(quarter) + "-"
    filter_col = [col for col in df if query in col]
    select_col = ["Tiêu chí"] + filter_col
    df_1 = df[select_col]

    s = df_1.iloc[2]
    n = ["Tổng tải sàn bình quân ", s[1]]
    for i in range(2, len(s)):
        n.append(int((s[i] + s[i - 1] + 1) / 2))

    new = pd.Series(n, index=df_1.columns)
    df1 = df_1.append(new, ignore_index=True)
    df2 = df1.iloc[[0, 1, 2, 13, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]]
    df2 = df2.reset_index(drop=True)

    df3 = get_eps(name)
    df3.columns = df2.columns

    data = pd.concat([df2, df3], axis=0).reset_index(drop=True)

    Path(f"{name}").mkdir(parents=True, exist_ok=True)
    data.to_csv(f"{name}/{name}.csv", mode='a', index_label="tieu chi")
    # my_file = Path("output.xlsx")
    # if my_file.is_file():
    #     with pd.ExcelWriter('output.xlsx', mode='a') as writer:
    #         data.to_excel(writer, sheet_name=name)
    # else:
    #     data.to_excel("output.xlsx", sheet_name=name)


if __name__ == '__main__':
    url = "https://s.cafef.vn/bao-cao-tai-chinh/G36/BSheet/2021/4/0/0/bao-cao-tai-chinh-tong-cong-ty-36-ctcp.chn"
    name = "G36"
    quarter = 3
    start = 2017
    end = 2021
    wrap(url, name)
