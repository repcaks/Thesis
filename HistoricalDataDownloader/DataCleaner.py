import pandas as pd
import os


def merge_stock_data(nyse, nasdaq):
    pd_array = [pd.read_csv(nyse), pd.read_csv(nasdaq)]
    all = pd.concat(pd_array)
    all = all.dropna()
    all.to_csv("Data/Cleaned/stock-all.csv", index=False)


def nyse_cleaner(dirname):
    files = os.listdir(dirname)
    pd_array = []

    for file in files:
        data = pd.read_csv('Data/NYSE/' + file)
        pd_array.append(data)

    df = pd.concat(pd_array)
    df = df.rename(columns={'Unnamed: 0': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})

    df.to_csv("Data/Cleaned/CleanedNyse/NyseAll.csv", index=False)


def nasdaq_cleaner(dirname):
    files = os.listdir(dirname)
    pd_array = []

    for file in files:
        data = pd.read_csv('Data/NASDAQ/' + file)
        pd_array.append(data)

    df = pd.concat(pd_array)
    df = df.rename(columns={'Unnamed: 0': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})

    df.to_csv("Data/Cleaned/CleanedNasdaq/NasdaqAll.csv", index=False)


def crypto_cleaner(dirname):
    files = os.listdir(dirname)
    pd_array = []

    for file in files:
        data = pd.read_csv('Data/Top100_Crypto/' + file)
        pd_array.append(data)

    df = pd.concat(pd_array)
    df = df.rename(columns={'Unnamed: 0': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})

    df.to_csv("Data/Cleaned/CleanedCrypto/CryptoAll.csv", index=False)


def gold_cleaner(filename):
    df = pd.read_csv(filename)
    df = df.rename(columns={'Unnamed: 0': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})
    df = df.dropna()
    df.to_csv("Data/Cleaned/Gold.csv", index=False)


def oil_cleaner(filename):
    df = pd.read_csv(filename)
    # df.index.name = "Id"
    # df.index = df.index + 1
    df = df.rename(columns={'Unnamed: 0': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})
    df = df.dropna()
    df.to_csv("Data/Cleaned/Oil.csv", index=False)


def news_cleaner(filename):
    df = pd.read_csv(filename, delimiter=';')
    df['published_date'] = pd.to_datetime(df['published_date']).dt.date
    df.to_csv("Data/Cleaned/News.csv", index=False)


def covid_cleaner(filename):
    df = pd.read_excel(
        os.path.join(filename),
        engine='openpyxl',
    )
    df.index.name = "id"
    df.index = df.index + 1
    df = df.drop(['day', 'month', 'year', 'countriesAndTerritories'], axis=1)
    df = df.rename(columns={"dateRep": "date", "countryterritoryCode": "Country", "geoId": "countryID",
                            "popData2019": "population", "continentExp": "Continent",
                            "Cumulative_number_for_14_days_of_COVID-19_cases_per_100000": "CasesPer100"})

    df.to_csv("Data/Covid.csv")
