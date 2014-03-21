#!/bin/bash

hadoop fs -test -e /kpi/20131129/2013112911/PW_*
if [ $? -ne 0 ]; then
    hadoop fs -mkdir -p /kpi/20131129/2013112911/PW_
else
    echo "PW EXIST"
fi

hadoop fs -test -e /kpi/20131129/2013112911/PT_*
if [ $? -ne 0 ]; then
    hadoop fs -mkdir -p /kpi/20131129/2013112911/PT_
else
    echo "PT EXIST"
fi
