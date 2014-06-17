#!/bin/bash

set -x
. ./conf.sh

export JAVA_HOME=/usr/local/jdk

$PYTHON_BIN run.py

ret=`echo $?`

if [ $ret -eq 0 ]; then
    echo "run.py success !!"
elif [ $ret -eq 1 ]; then
    echo "======================================================="
    echo "run.py <com.jobs.kpi.mapred.KPIDoaminMR> failed !!"
    echo "======================================================="
    exit 0
elif [ $ret -eq 2 ]; then
    echo "======================================================="
    echo "kpi_h.pig failed !!"
    echo "======================================================="
    exit 0
else
    echo "======================================================="
    echo $ret + "=========   other exception in run.py   ==========="
    echo "======================================================="
    exit 2
fi


