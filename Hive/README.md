Data sources: Yahoo,newscatcherapi,twitter,data.europa.eu

Yahoo removed api documentation but some informations are here: https://stackoverflow.com/questions/44030983/yahoo-finance-url-not-working

Price API Template. (Change TICKER in URL)
https://query1.finance.yahoo.com/v8/finance/chart/TICKER?symbol=TICKER&interval=1d

Examples:
Table Cryptocurrency:
daily BTC price: https://query1.finance.yahoo.com/v8/finance/chart/BTC-USD?symbol=BTC-USD&interval=1d

Table Stock:
daily Apple price: https://query1.finance.yahoo.com/v8/finance/chart/AAPL?symbol=AAPL&interval=1d

Table Company:
Apple Company info: https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=summaryProfile%2Cprice

Table Gold:
https://query1.finance.yahoo.com/v8/finance/chart/GC=F?symbol=GC=F&interval=1d

Table Oil:
https://query1.finance.yahoo.com/v8/finance/chart/CL=F?symbol=CL=F&interval=1d

Table IncomeStatement:
Apple income statement history quarterly :https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=incomeStatementHistoryQuarterly
Apple income statement history yearly: https://query1.finance.yahoo.com/v10/finance/quoteSummary/AAPL?modules=incomeStatementHistory

Table News:
https://newscatcherapi.com/news-api#news-api-pricing

Table Twitter:
https://developer.twitter.com/

Table Coronavirus:
https://data.europa.eu/euodp/en/data/dataset/covid-19-coronavirus-data