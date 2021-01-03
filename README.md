# Thesis
Stock market data analysis using Hortonworks Data Platform (HDP).

To become more familiar with HDP Sandbox: https://www.cloudera.com/tutorials/getting-started-with-hdp-sandbox.html

Prerequisites:
1) Download docker images:
   

2) Download directory with docker image proxy generator: 
    2a)Open HDP\sandbox\proxy directory 
    2b)Open git bash
    2c)run: ./proxy-deploy.sh

Map sandbox ip to hostfile :
1) Windows key + R
2) Paste: %WinDir%\System32\Drivers\Etc
3) Add line : 127.0.0.1 localhost sandbox-hdp.hortonworks.com sandbox-hdf.hortonworks.com

Ambari login credentials:
u: admin
p: cloudera

Login to docker linux(centos 7) container with HDP:
Run CMD and paste:
ssh root@sandbox-hdp.hortonworks.com -p 2222
Password: cloudera


Overview:
Implementation real time streaming use case from Twitter to Apache Hive using Spark Streaming, Apache Kafka and Apache Nifi
Implementation ETL use case from REST Api (Yahoo finance) to Apache Hive using NiFi.
Price prediction module using LSTM neural network in PySpark, keras, sklearn
Data analytics using Tableau.
Data analytics using apache Zeppelin, PySpark and Hive. 




