from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
import pandas as pd
from DataCleaner import nyse_cleaner, nasdaq_cleaner,\
    gold_cleaner, oil_cleaner, merge_stock_data, crypto_cleaner

def main():
    tickers() #get tickers
    nyse_data_download()
    nasdaq_data_download()
    merge_stock_data("Data/Cleaned/CleanedNyse/NyseAll.csv", "Data/Cleaned/CleanedNasdaq/NasdaqAll.csv")
    gold_historical_data_download()
    oil_historical_data_download()
    crypto_historical_data_download()
    create_date_table()
    generateYahooCallsForShareETL()

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
    crypto_cleaner("Data/Top100_Crypto/")


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

    nyse_cleaner("Data/NYSE/")


def nasdaq_data_download():
    tickers = tickers_nasdaq()
    for ticker in tickers:
        try:
            data = si.get_data(ticker)
            data.head()
            data.to_csv("Data/NASDAQ/" + ticker + ".csv")
        except:
            print(ticker)
    nasdaq_cleaner("Data/NASDAQ/")

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
    gold_cleaner('Data/Gold/Gold.csv')

def oil_historical_data_download():
    data = get_data('CL=F')
    data.head()
    data.to_csv("Data/Oil/Oil.csv")
    oil_cleaner('Data/Oil/Oil.csv')


def generateYahooCallsForShareETL():
    tickers_other = open("tickers.txt", 'r')
    tickers_nyse = open('nyse.txt', 'r')
    output = open("api_urls.txt", "a")

    for line in tickers_other:
        tickers = line.split(",")
        for ticker in tickers:
            api_body = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&interval=1d\n".format(ticker)
            output.write(api_body)

    for ticker in tickers_nyse:
        api_body = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&interval=1d\n".format(ticker.rstrip())
        output.write(api_body)

    output.close()

if __name__ == "__main__":
    main()

