CREATE DATABASE IF NOT EXISTS StockMarket;
USE StockMarket;

CREATE EXTERNAL TABLE IF NOT EXISTS Cryptocurrency(date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT,ticker CHAR(5))
CLUSTERED BY(ticker) INTO 64 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/Cryptocurrency'
tblproperties('skip.header.line.count'='1');


-- CREATE EXTERNAL TABLE IF NOT EXISTS Share(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT,ticker CHAR(5))
-- CLUSTERED BY(ticker) INTO 64 BUCKETS
-- ROW FORMAT DELIMITED FIELDS TERMINATED by','
-- stored as textfile
-- LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/share'
-- tblproperties('skip.header.line.count'='1');

-- load data local inpath '/Data/shares.csv' into table stockexchange.Share;

CREATE EXTERNAL TABLE IF NOT EXISTS Share(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT,ticker CHAR(5))
CLUSTERED BY(ticker) INTO 64 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/share'
tblproperties('skip.header.line.count'='1');

load data local inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/shares.orc' into table stockexchange.Share;


CREATE EXTERNAL TABLE IF NOT EXISTS Company(id INT,ticker CHAR(5),name STRING,address STRING,city STRING,zip STRING,industry STRING,exchange STRING, description STRING)
CLUSTERED BY(ticker) INTO 64 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/company'
tblproperties('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS IncomeStatement(ticker CHAR(5),date DATE,totalRevenue DOUBLE,costOfRevenue DOUBLE,grossProfit DOUBLE,netIncome DOUBLE,ticker CHAR(5))
CLUSTERED BY(ticker) INTO 64 BUCKETS 
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/incomestatement'
tblproperties('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS TimeDim(id int, date date, day int, week int, month int, quarter int, year int)
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/timedim'
tblproperties('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS Gold(date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/gold'
tblproperties('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS Oil(date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/oil'
tblproperties('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS News(topic STRING,link STRING,domain STRING,date Date,title STRING,lang STRING)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/news'
tblproperties('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS Twitter(content STRING,date Date,source STRING, url STRING,ticker CHAR(5))
ROW FORMAT SERDE
 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/twitter'
tblproperties('skip.header.line.count'='1');


CREATE EXTERNAL TABLE IF NOT EXISTS Coronavirus(date DATE,cases INT,deaths INT,country STRING,countryID CHAR(5),continent STRING,CasesPer100 DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED by','
stored as textfile
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/coronavirus'
tblproperties('skip.header.line.count'='1');

-- Tables used by ETL or other services:

CREATE TABLE IF NOT EXISTS Share_etl(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT,ticker CHAR(5));

INSERT INTO TABLE share SELECT * from ShareETL;
--testing query: insert into stockexchange.test values(from_unixtime(1609165800, 'yyyy-MM-dd'),1.0,1.0,1.0,1.0,1.0,1,"test");

CREATE TABLE IF NOT EXISTS ApplePricePredictionTmp
AS select * from Share where ticker="AAPL"
ORDER BY `date` asc;


CREATE TABLE IF NOT EXISTS Twitter(favourites_count STRING,id String,source String,text String,time String,user_account_created_time String,user_followers_count String,user_friends_count String,user_id String,user_name String,user_statuses_count String )


