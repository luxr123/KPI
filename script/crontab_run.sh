#!/bin/bash

set -x

. ./conf.sh

if [ $# -eq 1 ];
then
    date=$1
else
    date=`date -d yesterday +%Y%m%d`
fi

$HADOOP_BIN fs -create /log/$date
$HADOOP_BIN fs -put /home/hadoop/lu/kpi/data/train/train.txt /log/$date
nohup sh start_up_d.sh $date > "../log/start_up_d_"$date".log" 2>&1 &
