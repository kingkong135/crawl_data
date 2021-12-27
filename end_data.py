from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_data(name="G36", index=1):
    url = f'https://s.cafef.vn/Lich-su-giao-dich-{name}-1.chn'
    driver.get(url)

    df = pd.read_html(driver.page_source)[1]
    df = df.iloc[:, [0, 1]].drop([0, 1])
    df["date"] = pd.to_datetime(df[0], format="%d/%m/%Y")
    df["price"] = df[1].astype(float) * 1000
    df = df.drop([0, 1], axis=1)

    data = df[df.date.dt.month.isin([3, 6, 9, 12])]
    data = data[data.date.dt.day.isin([31, 30, 29])]

    for i in range(2, 66):
        try:
            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.XPATH, "//td//a[contains(text(),' >')]"))).click()
            # print(f"Clink page {i}")
            df = pd.read_html(driver.page_source)[1]
            df = df.iloc[:, [0, 1]].drop([0, 1])
            df["date"] = pd.to_datetime(df[0], format="%d/%m/%Y")
            df["price"] = df[1].astype(float) * 1000
            df = df.drop([0, 1], axis=1)
            df = df[df.date.dt.month.isin([3, 6, 9, 12])]
            df = df[df.date.dt.day.isin([31, 30, 29, 28])]
            data = pd.concat([data, df], axis=0)
        except TimeoutException:
            print("No more pages")
            break

    data = data.groupby([data.date.dt.year, data.date.dt.month]).head(1)
    data.iloc[:, [0]] = data['date'].dt.strftime('%d-%m-%Y')
    print(data)
    data = data.reset_index(drop=True)
    data.astype({"price": int})
    data = data.T
    data = data[data.columns[::-1]]
    data.columns = data.loc['date']
    data = data.drop(data.index[0])


    for i in range(3, 15, 3):
        query = f"-{i:02d}"
        filter_col = [col for col in data if query in col]
        df_1 = data[filter_col]
        df_1.to_csv(f"data_{index}/{name}/{name}_price.csv", mode='a', index_label="date")

    return data


def get_folder(index=1):
    # ck = pd.read_excel('ck.xlsx', sheet_name=str(index))
    ck = pd.read_csv('ck1.csv')
    fails = []
    for i in range(len(ck)):
        name = ck.loc[i, 'name']
        print(f"Folder {index} - Crawl data {i}: {name}")
        Path(f"data_{index}/{name}").mkdir(parents=True, exist_ok=True)
        try:
            get_data(name, index)
        except:
            print(f"faile name: {name}")
            fails.append(name)

    with open(f"fails_{index}.txt", 'w') as f:
        for item in fails:
            f.write("%s\n" % item)


if __name__ == '__main__':
    options = Options()
    options.add_argument('--headless')
    # driver = webdriver.Firefox(options=options)
    driver = webdriver.Firefox()
    arr = ["TIG", "GKM", "VC2", "CVN", "KSQ", "MAC", "NRC", "VC3", "HBS", "IVS", "NVB", "NTP"]
    for i in arr:
        print(i)
        get_data(i, 5)
    # get_data("HAS", 1)

    # for i in range(1, 4):
    #     get_folder(i)

    driver.close()
