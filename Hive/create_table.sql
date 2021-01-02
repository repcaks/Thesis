CREATE DATABASE IF NOT EXISTS StockMarket;

USE StockMarket;

CREATE EXTERNAL TABLE IF NOT EXISTS Cryptocurrency(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT,ticker CHAR(10))
CLUSTERED BY(ticker) INTO 64 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/Cryptocurrency'

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/crypto.orc' into table stockexchange.cryptocurrency;


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
--tblproperties('skip.header.line.count'='1');

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/share.orc' into table stockexchange.Share;

--tmp table for etl purposes:
CREATE TABLE IF NOT EXISTS etl_share(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT,ticker CHAR(5));

insert into share select * from etl_share;
truncate table share_etl;

-----------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS Company(ticker CHAR(5),name STRING,address STRING,city STRING,zip STRING,sector STRING,`exchange` STRING)
CLUSTERED BY(ticker) INTO 64 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/company'


load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/companies.orc' into table stockexchange.company;

------------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS Income_Statement(ticker CHAR(5),`date` DATE,totalRevenue DOUBLE,costOfRevenue DOUBLE,grossProfit DOUBLE,netIncome DOUBLE)
CLUSTERED BY(ticker) INTO 64 BUCKETS
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/incomestatement'

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/incomeStatements.orc' into table stockexchange.incomestatement;

-----------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS TimeDim(id int, `date` date, day int, week int, month int, quarter int, year int)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/timedim'


load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/dates.orc' into table stockexchange.timedim;

-----------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS Gold(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/gold'

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/gold.orc' into table stockexchange.gold;

-----------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS Oil(`date` DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, adjclose DOUBLE, volume INT)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/oil'
tblproperties('skip.header.line.count'='1');

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/oil.orc' into table stockexchange.oil;

-----------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS News(topic STRING,link STRING,domain STRING,`date` Date,title STRING,lang STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/news'


load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/news.orc' into table stockexchange.news;

-----------------------------------------------------

CREATE TABLE IF NOT EXISTS Twitter(`date` DATE,id STRING,source STRING,user_account_created_time STRING, user_favourites_count INT,user_followers_count INT, user_friends_count INT,user_id STRING, user_name STRING,user_statuses_count STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/twitter'
TBLPROPERTIES (
"  'bucketing_version'='2', "
"  'transactional'='true', "
"  'transactional_properties'='default', "
  'transient_lastDdlTime'='1609366081')

-----------------------------------------------------

CREATE EXTERNAL TABLE IF NOT EXISTS Coronavirus(id INT,`date` DATE,cases INT,deaths INT,country STRING,countryID CHAR(5),population INT, continent STRING,CasesPer100 DOUBLE)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION 'hdfs://sandbox-hdp.hortonworks.com:8020/warehouse/tablespace/managed/hive/stockexchange.db/coronavirus'

load data inpath 'hdfs://sandbox-hdp.hortonworks.com:8020/user/hive/tmp/data/covid.orc' into table stockexchange.coronavirus;


--Additional tables used by other services:

CREATE TABLE IF NOT EXISTS ApplePricePredictionTmp
AS select * from Share where ticker="AAPL"
ORDER BY `date` asc;



