#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys
import datetime
import time
import conf

debug = False

def run_cmd(cmd):
    global debug
    print(cmd)
    if debug:
        pass
    else:
        os.system(cmd)

if __name__ == '__main__':
    hadoop_conf = conf.get_conf('../conf/hadoop.conf', '=')
    hadoop_bin = hadoop_conf['HADOOP_BIN']
    pig_bin = hadoop_conf['PIG_BIN']
    hadoop_jar = hadoop_conf['HADOOP_JAR']
    hadoop_kpi = hadoop_conf['HADOOP_KPI']
    hadoop_log = hadoop_conf['HADOOP_LOG']
    hadoop_log_file_name = hadoop_conf['HADOOP_LOG_FILE_NAME']

    t_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    t_time = (datetime.datetime.now() - datetime.timedelta(days=1,hours=1)).strftime('%Y%m%d%H')
 
    input_path = hadoop_log + '/' + t_day + '/' + hadoop_log_file_name + t_time
    output_path = hadoop_kpi + '/' + t_day + '/' + t_time

    #     delete the output_path
    cmd = hadoop_bin + ' fs -rm -r ' + output_path
    print(cmd)
#    run_cmd(cmd)    

    #    execute the mapredcue jar
    cmd = hadoop_bin + ' jar ' + hadoop_jar + ' com.jobs.kpi.mapred.KPIDoaminMR ' + \
        input_path + ' ' +  output_path

    print(cmd)
    a = os.system(cmd)
    if a == 0:
        print('***************************************************')
        print(cmd + ' success!!')
        print('***************************************************')
    else:
        print('***************************************************')
        print(cmd + ' failed!!')
        sys.exit(1)
        print('***************************************************')
    
    #    Determine the hdfs file exists
    a_pu = os.system('hadoop fs -test -e ' + output_path + '/PU_*')
    if a_pu != 0:
        os.system('hadoop fs -mkdir -p ' + output_path + '/PU_')

    a_pt = os.system('hadoop fs -test -e ' + output_path + '/PT_*')
    if a_pt != 0:
        os.system('hadoop fs -mkdir -p ' + output_path + '/PT_')

    #    execute pig script
    cmd = pig_bin + ' -param inputPU=' + output_path + '/PU*' + ' -param inputPT=' + output_path + '/PT*' \
        + ' -param time=' + t_time + ' kpi_h.pig'
    print(cmd)
    b = os.system(cmd)
    if b == 0:
        print('***************************************************')
        print(cmd + ' success!!')
        print('***************************************************')
    else:
        print('***************************************************')
        print(cmd + ' failed!!')
        sys.exit(2)
        print('***************************************************')

