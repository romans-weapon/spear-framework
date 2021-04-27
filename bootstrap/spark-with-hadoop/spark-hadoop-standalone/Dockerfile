FROM centos:7.8.2003

ARG hadoop_version=2.10.1 
ARG spark_version=2.4.7 
ARG hive_version=2.1.1

#pre-requisites
RUN yum install  -y  curl bash java-1.8.0-openjdk-devel openssh-server openssh-clients which wget vim postgresql-jdbc git && \
 curl https://bintray.com/sbt/rpm/rpm |  tee /etc/yum.repos.d/bintray-sbt-rpm.repo && yum install sbt

################################################### Hadoop-client installation ###################################################################
RUN curl https://archive.apache.org/dist/hadoop/core/hadoop-${hadoop_version}/hadoop-${hadoop_version}.tar.gz -o hadoop.tgz && \
    tar -xzf hadoop.tgz && \ 
    mv hadoop-${hadoop_version} /usr/bin/ && \
    rm hadoop.tgz
    
# set Hadoop environment variables
ENV JAVA_HOME /usr/lib/jvm/java-1.8.0-openjdk
ENV HADOOP_HOME /usr/bin/hadoop-${hadoop_version}
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV HADOOP_MAPRED_HOME $HADOOP_HOME 
ENV HADOOP_COMMON_HOME $HADOOP_HOME 
ENV HADOOP_HDFS_HOME $HADOOP_HOME 
ENV HADOOP_COMMON_LIB_NATIVE_DIR $HADOOP_HOME/lib/native
ENV YARN_HOME $HADOOP_HOME
ENV PATH $JAVA_HOME/bin:$PATH
ENV PATH $HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH    

# set hadoop-env.sh
RUN echo "export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk" >> /usr/bin/hadoop-${hadoop_version}/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_HOME=/usr/bin/hadoop-${hadoop_version}" >> /usr/bin/hadoop-${hadoop_version}/etc/hadoop/hadoop-env.sh && \
    echo "export HADOOP_CONF_DIR=/usr/bin/hadoop-${hadoop_version}/etc/hadoop" >> /usr/bin/hadoop-${hadoop_version}/etc/hadoop/hadoop-env.sh

# create folders for nodes
RUN mkdir -p /usr/bin/data/nameNode /usr/bin/data/dataNode /usr/bin/data/nameNodeSecondary /usr/bin/data/tmp /usr/bin/hadoop-${hadoop_version}/logs
ADD configs/core-site.xml $HADOOP_HOME/etc/hadoop/core-site.xml
ADD configs/hdfs-site.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
ADD configs/mapred-site.xml $HADOOP_HOME/etc/hadoop/mapred-site.xml
####################################################### Hadoop client install end ##########################################################################



##################################################### spark-standalone without Hadoop #####################################################################

RUN curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-without-hadoop.tgz -o spark.tgz && \
    tar -xzf spark.tgz && \ 
    mv spark-${spark_version}-bin-without-hadoop /usr/bin/ && \
    mkdir /usr/bin/spark-${spark_version}-bin-without-hadoop/logs && \
    rm spark.tgz

# set environment variables
ENV SPARK_HOME /usr/bin/spark-${spark_version}-bin-without-hadoop
ENV SPARK_LOG_DIR /usr/bin/spark-${spark_version}-bin-without-hadoop/logs
ENV PATH $SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
RUN export SPARK_DIST_CLASSPATH=$(hadoop classpath)

RUN mv /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-env.sh.template /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-env.sh && \
 echo "export SPARK_DIST_CLASSPATH=$(hadoop classpath)" >> /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-env.sh && \
 echo "export SPARK_LOG_DIR=/usr/bin/spark-${spark_version}-bin-without-hadoop/logs" >> /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-env.sh
RUN mv /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-defaults.conf.template /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-defaults.conf && \
 echo "spark.eventLog.dir file:/usr/bin/spark-${spark_version}-bin-without-hadoop/logs" >> /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-defaults.conf && \
 echo "spark.history.fs.logDirectory file:/usr/bin/spark-${spark_version}-bin-without-hadoop/logs" >> /usr/bin/spark-${spark_version}-bin-without-hadoop/conf/spark-defaults.conf
########################################################spark-standalone installation end###########################################################################################



###########################################################hive client installation ###################################################################################################
RUN curl  https://archive.apache.org/dist/hive/hive-${hive_version}/apache-hive-${hive_version}-bin.tar.gz -o hive.tgz && \
    tar -xzf hive.tgz && \
    mv apache-hive-${hive_version}-bin /usr/bin/ && \
    rm -rf hive.tgz

# set Hive environment variables
ENV HIVE_HOME /usr/bin/apache-hive-${hive_version}-bin
ENV PATH $HIVE_HOME/bin:$PATH
RUN cp /usr/share/java/postgresql-jdbc3.jar /usr/bin/apache-hive-${hive_version}-bin/lib/
ADD configs/hive-site.xml $HIVE_HOME/conf/hive-site.xml
RUN cp $HIVE_HOME/conf/hive-site.xml $SPARK_HOME/conf/
RUN echo "export HADOOP_HOME=/usr/bin/hadoop-${hadoop_version}" >> /usr/bin/hadoop-${hadoop_version}/bin/hive-config.sh

#setting ssh key 
RUN ssh-keygen -f $HOME/.ssh/id_rsa -t rsa -N "" && \
 cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

ADD configs/services.sh /usr/bin/services.sh
ADD configs/config /usr/bin/config
RUN mv /usr/bin/config $HOME/.ssh/ && chmod 600 $HOME/.ssh/config
############################################################hive installtion end###########################################################################################
