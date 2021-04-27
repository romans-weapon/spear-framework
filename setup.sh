#! /bin/bash

set -e

cd spear-framework/ || { echo "ERROR:No such directory"
exit 1
}

# build hive-postgres metastore image
{
  echo "[$(date)]        INFO:[+]Building image: hive-metastore"
  docker build . -t hive-metastore:latest -f bootstrap/spark-with-hadoop/hive-metastore/Dockerfile
} && echo "[$(date)]        INFO:[+]Building image hive-metastore: SUCCESS" || {
  echo "[$(date)]        ERROR:[+]Building image for hive-metastore :FAILED"
  exit 1
}

# build spark-hadoop standalone metastore image
{
  echo "[$(date)]        INFO:[+]Building image: spark-hadoop-standalone"
  docker build . -t spark-standalone-hadoop:latest -f bootstrap/spark-with-hadoop/spark-hadoop-standalone/Dockerfile
} && echo "[$(date)]        INFO:[+]Building image spark-hadoop-standalone: SUCCESS" || {
  echo "[$(date)]        ERROR:[+]Building image for spark-hadoop-standalone :FAILED"
  exit 1
}

#spinning docker containers using compose
docker-compose -f bootstrap/spark-with-hadoop/docker-compose.yml up -d

sleep 60
#starting hadoop
echo "Starting services"
docker exec -it spark bash -c "hdfs namenode -format && start-dfs.sh && hdfs dfs -mkdir -p /tmp && hdfs dfs -mkdir -p /user/hive/warehouse && hdfs dfs -chmod g+w /user/hive/warehouse" &&
docker exec -d spark bash -c "hive --service metastore && hive --service hiveserver2"
docker exec -it spark bash -c "cd /opt && git clone git@github.com:AnudeepKonaboina/spear-framework.git && cd spear-framework && sbt package"
docker exec -it spark bash -c "cd /opt/spear-framework/target/scala-2.12 && spark-shell --jars spear-framework_2.12-0.1.jar --packages  "
