# Problem 7: 
#
#   This step comprises of three substeps. Please perform tasks under each subset completely  
#   using sqoop pull data from MYSQL orders table into /user/cloudera/problem7/prework as AVRO data file using only one mapper
#   Pull the file from \user\cloudera\problem7\prework into a local folder named flume-avro
#   create a flume agent configuration such that it has an avro source at localhost and port number 11112,  a jdbc channel and an hdfs file sink at /user/#   #cloudera/problem7/sink
#   Use the following command to run an avro client flume-ng avro-client -H localhost -p 11112 -F <<Provide your avro file path here>>


##Invoke the packages needed and set the context
from pyspark import SparkContext,SparkConf
import os
conf=SparkConf().setMaster("local").setAppName("pyspark")
sc=SparkContext(conf=conf)

# Sqoop data from the mysql database
# "orders" table is now imported into hdfs in avro-format
# 
sqoop import \
--connect "jdbc:mysql://quickstart.cloudera:3306/retail_db" \
--username retail_dba \
--password cloudera \
--table orders \
--as-avrodatafile \
--m 1 \
--target-dir /user/cloudera/problem7/prework

#Invoke a shell command from the Pyspark shell 
os.system("mkdir /user/cloudera/flume_avro")
os.system("cd /user/cloudera/flume_avro")
os.system("hdfs dfs -copyToLocal /user/cloudera/problem7/prework/part-m-00000.avro")

## start the flume agent 
os.system("flume-ng agent --name a1 --conf-file flume_avro.conf")

## Run the avro client tool to pump data to the flume agent
os.system("flume-ng avro-client -H localhost -p 11112 -F /home/cloudera/flume_avro/part-m-00000.avro")