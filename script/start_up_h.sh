#!/bin/bash

set -x
. ./conf.sh

export JAVA_HOME=/usr/local/jdk

$PYTHON_BIN run_h.py

ret=`echo $?`

if [ $ret -eq 0 ]; then
    echo "run.py success !!"
elif [ $ret -ge 1 ]; then
    echo "======================================================="
    echo "run_h.py has some exception"
    echo "run_h.py get the Exception Score -- "$ret
    echo "======================================================="
    exit 0
else
    echo "======================================================="
    echo $ret + "=========   other exception in run.py   ========"
    echo "======================================================="
    exit 2
fi


