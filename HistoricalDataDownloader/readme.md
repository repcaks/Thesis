Scripts to collect data which is used in the project. Full data download and clean takes 2-3 hours. Program collects data in a csv format, but tables in Hive needs orc format (it is possible to use csv, but orc has a better performance). To get orc file run pyspark script in Zeppelin.


"Data" directory--> paste this directory tree to project folder next to Main.py and DataCleaner

! yahoo_fin doesn't work with python 3.6, Use python 3.9 !

1)install libs
pip install requests yahoo_fin pandas requests_html, openpyxl, xlrd

2)To get stock,shares,oil,gold and clean data run Main.py.

    2.1)generateYahooCallsForShareETL()  --> Method for share table ETL process.

3)To clean covid data (downloaded from: https://www.ecdc.europa.eu/ and renamed to "covid.xlsx" ) add file to project main directory and run commented method in Main.py : covid_cleaner("covid.xlsx")
