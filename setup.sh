#! /bin/bash

set -e
echo "[$(date)]        INFO:[+]Spear-framework setup status     [started]"
{
  docker-compose up -d && sleep 60 &&
    #starting hadoop and hive services
    docker exec -it spear bash -c "hdfs namenode -format && start-dfs.sh && hdfs dfs -mkdir -p /tmp && hdfs dfs -mkdir -p /user/hive/warehouse && hdfs dfs -chmod g+w /user/hive/warehouse" &&
    docker exec -it spear bash -c "sed '90 a figlet -f slant -w 100 Spear Framework' /usr/bin/spark-2.4.7-bin-without-hadoop/bin/spark-shell > /usr/bin/spark-2.4.7-bin-without-hadoop/bin/spark-shell-test && yum install -y epel-release figlet && yes | cp /usr/bin/spark-2.4.7-bin-without-hadoop/bin/spark-shell-test /usr/bin/spark-2.4.7-bin-without-hadoop/bin/spark-shell" &&
    docker exec -d spear bash -c "hive --service metastore && sleep 15 && hive --service hiveserver2 && sleep 20 && hive -e 'set mapreduce.framework.name=local;'" &&
    docker exec -it spear bash -c "chmod u+x /root/spear-shell.sh && wget https://mirrors.estointernet.in/apache/kafka/2.7.0/kafka_2.13-2.7.0.tgz -O /tmp/kafka.tgz && cd / && tar -xvf /tmp/kafka.tgz -C / && mv kafka_2.13-2.7.0 kafka && rm -f /etc/yum.repos.d/bintray-rpm.repo &&  curl -L https://www.scala-sbt.org/sbt-rpm.repo > sbt-rpm.repo && mv sbt-rpm.repo /etc/yum.repos.d/ && yum install -y sbt && cd /opt && git clone https://github.com/AnudeepKonaboina/spear-framework.git && cd spear-framework && sbt 'set test in assembly := {}' clean assembly && cp /etc/jars/* /opt/spear-framework/target/scala-*/" &&
    echo "[$(date)]        INFO:[+]Spear-framework setup status     [success]"
} ||
  {
    echo "[$(date)]        INFO:[+]Spear-framework setup status      [failed]"
    exit 1
  }
