#!/bin/bash

# set -x

#export JAVA_HOME=/usr/local/jdk
#export JRE_HOME=/usr/local/jre
#export HADOOP_INSTALL=/home/hadoop/hadoop-2.2.0
#export YARN_HOME=/home/hadoop/hadoop-2.2.0/share/hadoop/mapreduce
#export HADOOP_COMMON_HOME=/home/hadoop/hadoop-2.2.0/share/hadoop/common
#export HADOOP_CONF_DIR=/home/hadoop/hadoop-2.2.0/etc/hadoop
#export HBASE_HOME=/home/hadoop/hbase-0.94
#export PIG_HOME=/home/hadoop/lu/pig-0.10.1

#export PIG_CLASSPATH=$HADOOP_COMMON_HOME/hadoop-common-2.2.0.jar:$YARN_HOME/*:$YARN_HOME/lib/*:$HADOOP_COMMON_HOME/*:$HADOOP_COMMON_HOME/lib/*:$HADOOP_CONF_DIR:$HBASE_HOME/conf:$HBASE_HOME/hbase-0.94.13.jar:$HBASE_HOME/lib/zookeeper-3.4.5.jar:$HBASE_HOME/lib/protobuf-java-2.5.0.jar

#export CLASSPATH=$JAVA_HOME/lib:$JAVA_HOME/lib/tools.jar:$JAVA_HOME/lib/dt.jar:$JRE_HOME/lib/rt.jar:$JRE_HOME/lib:$HADOOP_CONF_DIR:$HBASE_HOME/lib:$PIG_HOME:$PIG_HOME/lib:$PIG_CLASSPATH

#echo $CLASSPATH

#pig -Dpig.additional.jars=$PIG_CLASSPATH:/home/hadoop/hadoop-2.2.0/share/hadoop/common/hadoop-common-2.2.0.jar -param input="/kpi/20131127/2013112709" -param time="2013112709" test.pig
pig -param input="/kpi/20131127/2013112709" -param time="2013112709" test.pig
