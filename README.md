# Thesis
Stock market data analysis using Hortonworks Data Platform (HDP).<br />
To become more familiar with HDP Sandbox: https://www.cloudera.com/tutorials/getting-started-with-hdp-sandbox.html

Architecture:

![image](https://github.com/repcaks/Thesis/blob/main/Architecture.png)


Prerequisites:
1) Download docker images:
   

2) Download directory with docker image proxy generator: 
    1) Open HDP\sandbox\proxy directory 
    2) Open git bash
    3) run: ./proxy-deploy.sh

Map sandbox ip to hostfile :
1) Windows key + R
2) Paste: %WinDir%\System32\Drivers\Etc
3) Add line : 127.0.0.1 localhost sandbox-hdp.hortonworks.com sandbox-hdf.hortonworks.com

Ambari login credentials :
   1) u: admin
   2) p: cloudera

Login to docker linux(centos 7) container with HDP:
   1) Run CMD and paste:
   2) ssh root@sandbox-hdp.hortonworks.com -p 2222
   3) Password: cloudera


Overview:
1) Implementation real time streaming use case from Twitter to Apache Hive using Spark Streaming, Apache Kafka and Apache Nifi
2) Implementation ETL use case from REST Api (Yahoo finance) to Apache Hive using NiFi.
3) Price prediction module using LSTM neural network in PySpark, keras, sklearn
4) Data analytics using Tableau.
5) Data analytics using apache Zeppelin, PySpark and Hive. 




