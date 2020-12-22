from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
import pandas as pd


def main():
     tickers() #get tickers
     nyse_data_download()
     nasdaq_data_download()
     gold_historical_data_download()
     oil_historical_data_download()
     crypto_historical_data_download()
     create_date_table()


def tickers():
    tickers = tickers_sp500()
    with open('tickers.txt', 'w') as f:
        for item in tickers:
            f.write("%s," % item)

    data = get_top_crypto()
    symbol_col = data['Symbol']
    symbols = []

    for i in symbol_col:
        x = i.replace('-USD', '')
        symbols.append(x)

    with open('tickers.txt', 'a') as f:
        for item in symbols:
            f.write("%s," % item)

    name_col = data['Name']
    names = []
    
    for name in name_col:
        x = name.replace(' USD', '')
        names.append(x)

    with open('tickers.txt', 'a') as f:
        for item in names:
            f.write("%s," % item)


def daily_crypto_data_download():
    data = get_top_crypto()
    data.head()
    data.index.name = "Id"
    data.index = data.index + 1
    data.to_csv("Data/crypto.csv")


def create_date_table(start='2000-01-01', end='2050-12-31'):
    df = pd.DataFrame({"Date": pd.date_range(start, end)})
    df.index.name = "Id"
    df.index = df.index + 1
    df["Day"] = df.Date.dt.dayofweek
    df["Week"] = df.Date.dt.weekofyear
    df["Month"] = df.Date.dt.month
    df["Quarter"] = df.Date.dt.quarter
    df["Year"] = df.Date.dt.year
    df.to_csv("Data/Cleaned/Dates.csv")


def crypto_historical_data_download():
    data = get_top_crypto()
    print(data)
    for ticker in data[data.columns[0]]:
        try:
            data = si.get_data(ticker)
            data.head()
            data.to_csv("Data/Top100_Crypto/" + ticker + ".csv")
        except:
            print(ticker)


def nyse_data_download():
    tickers = []
    with open('nyse.txt', 'r') as filehandle:
        for line in filehandle:
            tickers.append(line.replace('\n', ''))

    for ticker in tickers:
        try:
            data = si.get_data(ticker)
            data.head()
            data.to_csv("Data/NYSE/" + ticker + ".csv")
        except:
            print(ticker)


def nasdaq_data_download():
    tickers = tickers_nasdaq()
    for ticker in tickers:
        try:
            data = si.get_data(ticker)
            data.head()
            data.to_csv("Data/NASDAQ/" + ticker + ".csv")
        except:
            print(ticker)


def nasdaq_ticker_data_download(ticker):
        try:
            data = si.get_data(ticker)
            data.head()
            data.to_csv(ticker + ".csv")
        except:
            print('wrong '+ticker)


def gold_historical_data_download():
    data = get_data('GC=F')
    data.head()
    data.to_csv("Data/Gold/Gold.csv")


def oil_historical_data_download():
    data = get_data('CL=F')
    data.head()
    data.to_csv("Data/Oil/Oil.csv")


if __name__ == "__main__":
    main()

