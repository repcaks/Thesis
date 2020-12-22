import pandas as pd
import os


def merge_stock_data(nyse, nasdaq, crypto):

    pd_array = [pd.read_csv(nyse), pd.read_csv(nasdaq), pd.read_csv(crypto)]
    all = pd.concat(pd_array)
    all.reset_index(drop=True, inplace=True)
    all.index.name = "Id"
    all.index = all.index + 1
    all = all.dropna()
    all.to_csv("Data/Cleaned/stock-all.csv")


def nyse_cleaner(dirname):

    files = os.listdir(dirname)
    pd_array = []

    for file in files:
        data = pd.read_csv('Data/NYSE/'+file)
        pd_array.append(data)

    df = pd.concat(pd_array)
    df = df.rename(columns={'Unnamed: 0':'Date','open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})

    # df.reset_index(drop=True, inplace=True)
    # df.index.name = "Id"
    # df.index = df.index + 1

    df['Stock'] = pd.Series('NYSE', index=df.index)

    df.to_csv("Data/Cleaned/CleanedNyse/NyseAll.csv",index=False)


def nasdaq_cleaner(dirname):
    files = os.listdir(dirname)
    pd_array = []

    for file in files:
        data = pd.read_csv('Data/NASDAQ/'+file)
        pd_array.append(data)

    df = pd.concat(pd_array)
    df = df.rename(columns={'Unnamed: 0': 'Date', 'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})

    # df.reset_index(drop=True, inplace=True)
    # df.index.name = "Id"
    # df.index = df.index + 1

    df['Stock'] = pd.Series('NASDAQ', index=df.index)

    df.to_csv("Data/Cleaned/CleanedNasdaq/NasdaqAll.csv",index=False)


def crypto_cleaner(dirname):
    files = os.listdir(dirname)
    pd_array = []

    for file in files:
        data = pd.read_csv('Data/Top100_Crypto/'+file)
        pd_array.append(data)

    df = pd.concat(pd_array)
    df = df.rename(columns={'Unnamed: 0':'Date','open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})

    # df.reset_index(drop=True, inplace=True)
    # df.index.name = "Id"
    # df.index = df.index + 1

    df['Stock'] = pd.Series('Coinmarketcap', index=df.index)

    df.to_csv("Data/Cleaned/CleanedCrypto/CryptoAll.csv",index=False)


def gold_cleaner(filename):
    df = pd.read_csv(filename)
    df.index.name = "Id"
    df.index = df.index + 1
    df = df.rename(columns={'Unnamed: 0':'Date','open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})
    df = df.dropna()
    df.to_csv("Cleaned/Gold.csv")


def oil_cleaner(filename):
    df = pd.read_csv(filename)
    df.index.name = "Id"
    df.index = df.index + 1
    df = df.rename(columns={'Unnamed: 0':'Date','open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close',
                            'adjclose': 'Adjclose', 'volume': 'Volume', 'ticker': 'Ticker'})
    df = df.dropna()
    df.to_csv("Data/Cleaned/Oil.csv")


def news_cleaner(filename):
    df = pd.read_csv(filename)
    df['publish_date'] = pd.to_datetime(df['publish_date'].astype(str))
    df.index.name = "Id"
    df.index = df.index + 1
    df = df.rename(columns={"publish_date": "Date", "headline_text": "Title"})
    df = df.dropna()
    df.to_csv("Data/Cleaned/News.csv")


if __name__ == '__main__':
    crypto_cleaner("Data/Top100_Crypto/")
    nyse_cleaner("Data/NYSE/")
    nasdaq_cleaner("Data/NASDAQ/")
    merge_stock_data("Data/Cleaned/CleanedNyse/NyseAll.csv", "Data/Cleaned/CleanedNasdaq/NasdaqAll.csv", "Data/Cleaned/CleanedCrypto/CryptoAll.csv")
    news_cleaner("Data/abcnews-date-text.csv")
    gold_cleaner('Data/Gold/Gold.csv')
    oil_cleaner('Data/Oil/Oil.csv')

