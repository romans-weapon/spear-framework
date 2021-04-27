#! /bin/bash

set -e 

echo "Starting services.sh"

hdfs namenode -format && \
start-dfs.sh && \
hdfs dfs -mkdir -p /tmp && \
hdfs dfs -mkdir -p /user/hive/warehouse && \
hdfs dfs -chmod g+w /tmp && \
hdfs dfs -chmod g+w /user/hive/warehouse && \
hive --service metastore && \
hive --service hiveserver2  

