#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys
import datetime
import time
import conf
import threading

debug = False
#debug = True
exp_score = 0

def run_cmd(cmd):
    global debug
    print(cmd)
    if debug:
        pass
    else:
        res = os.system(cmd)
        return res

def loop(num, cmd, ctime):
    global exp_score
    try:
        res = run_cmd(cmd)
        if res == 0:
            print('\033[91m' + '=========================================' + '\033[0m')
            print(cmd, 'success!!')
            print('loop',num,'done takes:',(time.time() - ctime),'seconds')
            print('\033[91m' + '=========================================' + '\033[0m')
        else:
            raise Exception(cmd)
    except Exception as ex:
        exp_score += 1 << num
        print('\033[91m' + '=========================================' + '\033[0m')
        print('\nSome error/exception occurred.')
        print(Exception,":",ex)
        print(cmd, 'failed!!')
        print('\033[91m' + '=========================================' + '\033[0m')

def main():
    global exp_score
    hadoop_conf = conf.get_conf('../conf/hadoop.conf', '=')
    hadoop_bin = hadoop_conf['HADOOP_BIN']
    pig_bin = hadoop_conf['PIG_BIN']
    hadoop_jar = hadoop_conf['HADOOP_JAR']
    hadoop_kpi = hadoop_conf['HADOOP_KPI']
    hadoop_log = hadoop_conf['HADOOP_LOG']
    hadoop_log_file_name = hadoop_conf['HADOOP_LOG_FILE_NAME']

    t_day = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime('%Y%m%d')
    #t_day = time.strftime('%Y%m%d',time.localtime(time.time()))
    
    input_path = hadoop_log + '/' + t_day
    output_path = hadoop_kpi + '/' + t_day + '/' + t_day

    
    domain_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                           ' -p time=' + t_day + ' kpi_domain_d.pig'

    page_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                         ' -p time=' + t_day + ' kpi_page_d.pig'

    source_analysis_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                                    ' -p time=' + t_day + ' kpi_source_analysis.pig'

    region_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                           ' -p time=' + t_day + ' kpi_region_distribute.pig'

    hot_cmd = pig_bin + ' -p inputHeat=' + output_path + '/HeatMap' + ' -p inputHotLink=' + output_path + '/HotLink' + \
                        ' -p time=' + t_day + ' kpi_heatmap_d.pig'

    ads_cmd = pig_bin + ' -p inputAds=' + output_path + '/Ads' + ' -p inputAdsPT=' + output_path + '/AdsTimeLength' + \
                        ' -p time=' + t_day + ' kpi_ads_d.pig'

    loops_cmd = [domain_cmd, page_cmd, source_analysis_cmd, region_cmd, hot_cmd, ads_cmd]

    threads = []
    nloops= range(len(loops_cmd))

    for i in nloops:
        t = threading.Thread(target=loop, args=(i+1, loops_cmd[i], time.time()))
        threads.append(t)

    for i in nloops:#开始线程
        threads[i].setDaemon(True)
        threads[i].start()

    for i in nloops:        #等待所有的子线程执行结束(最多等待3秒,默认为None，无限等待)
        threads[i].join(timeout=None)
        print('loop',i+1,'is alive',threads[i].isAlive())
        print('loop',i+1,'is daemon',threads[i].isDaemon())


if __name__ == '__main__':
    main()

    ##########################################################################################################################################
    print('\033[91m' + '===============================================================================' + '\033[0m')
    print('\033[91m==========================' + 'Exception Score is %d================================\033[0m'%exp_score)
    print('\033[91m' + '===============================================================================' + '\033[0m')

