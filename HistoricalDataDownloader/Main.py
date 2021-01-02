from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
import pandas as pd
import requests
from requests.exceptions import HTTPError
from DataCleaner import *
import json
from jsonpath_ng import jsonpath, parse


def main():
    tickers() #get tickers
    nyse_data_download()
    nasdaq_data_download()
    merge_stock_data("Data/Cleaned/CleanedNyse/NyseAll.csv", "Data/Cleaned/CleanedNasdaq/NasdaqAll.csv")
    gold_historical_data_download()
    oil_historical_data_download()
    crypto_historical_data_download()
    create_date_table()
    create_company_table()
    create_income_statement_table()
    generate_yahoo_calls_for_share_ETL()
    covid_cleaner("covid.xlsx")
    news_cleaner("news.csv")


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
        print('wrong ' + ticker)


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


def generate_yahoo_calls_for_share_ETL():
    tickers_other = open("tickers.txt", 'r')
    tickers_nyse = open('nyse.txt', 'r')
    output = open("api_urls.txt", "a")

    for line in tickers_other:
        tickers = line.split(",")
        for ticker in tickers:
            api_body = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&interval=1d\n".format(ticker)
            output.write(api_body)

    for ticker in tickers_nyse:
        api_body = "https://query1.finance.yahoo.com/v8/finance/chart/{0}?symbol={0}&interval=1d\n".format(
            ticker.rstrip())
        output.write(api_body)

    output.close()


def create_company_table():
    tickers = []

    tickers_nasd = tickers_nasdaq()
    with open('nyse.txt', 'r') as filehandle:
        for line in filehandle:
            tickers.append(line.replace('\n', ''))

    all = tickers + tickers_nasd

    file = open("companies.csv", "a")
    for ticker in all:
        company_info_url = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/" + ticker + "?modules=summaryProfile%2Cprice"

        try:
            response = requests.get(company_info_url)
            response.raise_for_status()
            json_response = response.json()

            json_data = json.loads(json.dumps(json_response))

            ticker = parse('$.quoteSummary.result[0].price.symbol').find(json_data)
            name = parse('$.quoteSummary.result[0].price.longName').find(json_data)
            address = parse('$.quoteSummary.result[0].summaryProfile.address1').find(json_data)
            city = parse('$.quoteSummary.result[0].summaryProfile.city').find(json_data)
            zip_code = parse('$.quoteSummary.result[0].summaryProfile.zip').find(json_data)
            sector = parse('$.quoteSummary.result[0].summaryProfile.sector').find(json_data)
            exchange = parse('$.quoteSummary.result[0].price.exchangeName').find(json_data)
            row = ticker[0].value.replace(",", "") + "," + name[0].value.replace(",", "") + "," + address[
                0].value.replace(",", "") + "," + city[0].value.replace(",", "") + "," + zip_code[
                      0].value.replace(",", "") + "," + sector[0].value.replace(",", "") + "," + exchange[
                      0].value.replace(",", "")

            file.write(row)
            file.write("\n")

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
    file.close()


def create_income_statement_table():
    tickers = []

    tickers_nasd = tickers_nasdaq()
    with open('nyse.txt', 'r') as filehandle:
        for line in filehandle:
            tickers.append(line.replace('\n', ''))

    all = tickers + tickers_nasd

    file = open("incomeStatements.csv", "a")

    for ticker in all:
        company_info_url = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/" + ticker + "?modules=incomeStatementHistoryQuarterly"

        try:
            response = requests.get(company_info_url)
            response.raise_for_status()
            json_response = response.json()

            json_data = json.loads(json.dumps(json_response))

            dates, total_revenue, cost_of_revenue, gross_profit, net_income = [], [], [], [], []
            for _ in range(4):
                dates.append(parse(
                    '$.quoteSummary.result[0].incomeStatementHistoryQuarterly.incomeStatementHistory[' + str(
                        _) + '].endDate.fmt').find(json_data)[0].value)

                total_revenue.append(
                    parse('$.quoteSummary.result[0].incomeStatementHistoryQuarterly.incomeStatementHistory[' + str(
                        _) + '].totalRevenue.raw').find(json_data)[0].value)

                cost_of_revenue.append(
                    parse('$.quoteSummary.result[0].incomeStatementHistoryQuarterly.incomeStatementHistory[' + str(
                        _) + '].costOfRevenue.raw').find(json_data)[0].value)

                gross_profit.append(
                    parse('$.quoteSummary.result[0].incomeStatementHistoryQuarterly.incomeStatementHistory[' + str(
                        _) + '].grossProfit.raw').find(json_data)[0].value)

                net_income.append(
                    parse('$.quoteSummary.result[0].incomeStatementHistoryQuarterly.incomeStatementHistory[' + str(
                        _) + '].netIncome.raw').find(json_data)[0].value)

            for _ in range(4):
                row = ticker + "," + dates[_] + "," + str(total_revenue[_]) + "," + str(cost_of_revenue[_]) + "," + str(
                    gross_profit[_]) + "," + str(net_income[_])

                file.write(row)
                file.write("\n")

        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')
    file.close()


if __name__ == "__main__":
    main()
