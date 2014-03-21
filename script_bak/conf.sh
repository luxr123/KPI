#!/bin/bash
ROOT=/home/hadoop/lu/kpi
CONF=$ROOT/conf
OUTPUT=$ROOT/output
DATA=$ROOT/data
LOG=$ROOT/log

HADOOP_ROOT=`cat $CONF/hadoop.conf | grep 'HADOOP_ROOT' | awk -F"=" '{print $2}'`
HADOOP_OUTPUT=$HADOOP_ROOT/tmp
HADOOP_JOB_NAME=`cat $CONF/hadoop.conf | grep 'HADOOP_JOB_NAME' | awk -F"=" '{print $2}'`
HADOOP_HOME=`cat $CONF/hadoop.conf | grep 'HADOOP_HOME' | awk -F"=" '{print $2}'`
HADOOP_BIN=`cat $CONF/hadoop.conf | grep 'HADOOP_BIN' | awk -F"=" '{print $2}'`
HADOOP_STREAMING=`cat $CONF/hadoop.conf | grep 'HADOOP_STREAMING' | awk -F"=" '{print $2}'`
PIG_BIN=`cat $CONF/hadoop.conf | grep 'PIG_BIN' | awk -F"=" '{print $2}'`

PYTHON_BIN=`cat $CONF/hadoop.conf | grep 'PYTHON_BIN' | awk -F"=" '{print $2}'`

RED_NUM=`cat $CONF/*.conf | grep 'RED_NUM' | awk -F"=" '{print $2}'`

LIB_DIR=`cat $CONF/*.conf | grep 'LOCAL_LIB_DIR' | awk -F"=" '{print $2}'`
