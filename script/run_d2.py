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
        res = os.system(cmd)
        return res

if __name__ == '__main__':
    hadoop_conf = conf.get_conf('../conf/hadoop.conf', '=')
    hadoop_bin = hadoop_conf['HADOOP_BIN']
    pig_bin = hadoop_conf['PIG_BIN']
    hadoop_jar = hadoop_conf['HADOOP_JAR']
    hadoop_kpi = hadoop_conf['HADOOP_KPI']
    hadoop_log = hadoop_conf['HADOOP_LOG']
    hadoop_log_file_name = hadoop_conf['HADOOP_LOG_FILE_NAME']

    # t_day = (datetime.datetime.now() - datetime.timedelta(days=0)).strftime('%Y%m%d')
    t_day = time.strftime('%Y%m%d',time.localtime(time.time()))
    
    input_path = hadoop_log + '/' + t_day
    output_path = hadoop_kpi + '/' + t_day + '/' + t_day

    ##     delete the output_path
    cmd = hadoop_bin + ' fs -rm -r ' + output_path
    run_cmd(cmd)    

    exp_score = 0
    ##    execute the mapredcue jar
    cmd = hadoop_bin + ' jar ' + hadoop_jar + ' com.jobs.kpi.mapred.KPIDoaminMR ' + \
        input_path + ' ' +  output_path
    try:
        a = run_cmd(cmd)
        if a == 0:
            print('***************************************************')
            print(cmd + ' success!!')
            print('***************************************************')
    except Exception as ex:
        exp_score += 1 << 1
        print('***************************************************')
        print('\nSome error/exception occurred.')
        print(Exception,":",ex)
        print('***************************************************')
        sys.exit(1)
    
    ##    Determine the hdfs file exists
    pu = run_cmd('hadoop fs -test -e ' + output_path + '/PU_*')
    if pu != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/PU_')

    pt = run_cmd('hadoop fs -test -e ' + output_path + '/PT_*')
    if pt != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/PT_')

    acc = run_cmd('hadoop fs -test -e ' + output_path + '/HeatMap')
    if acc != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/HeatMap')

    ads = run_cmd('hadoop fs -test -e ' + output_path + '/Ads')
    if ads != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/Ads')
    
    hot = run_cmd('hadoop fs -test -e ' + output_path + '/HotLink')
    if hot != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/HotLink')

    #    execute domain pig script
    # pig -param inputPU=/kpi/20140102/20140102/PU* -param inputPT=/kpi/20140102/20140102/PT* -param time=20140102 kpi_domain_d.pig
    domain_cmd = pig_bin + ' -param inputPU=' + output_path + '/PU*' + ' -param inputPT=' + output_path + '/PT*' + \
                        ' -param time=' + t_day + ' adv_kpi_domain_d.pig'
    try:
        res_domain = run_cmd(domain_cmd)
        if res_domain == 0:
            print('***************************************************')
            print(domain_cmd + ' success!!')
            print('***************************************************')
    except Exception as ex:
        exp_score += 1 << 2
        print('***************************************************')
        print('\nSome error/exception occurred.')
        print(Exception,":",ex)
        print(domain_cmd + ' failed!!')
        print('***************************************************')

