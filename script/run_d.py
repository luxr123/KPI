#!/usr/bin/env python
# -*- coding: utf-8 -*-
###################################  
# @author luxury
# @date 2014-03-20  
##################################

import os,sys
import datetime
import time
import conf
import threading
import logging
import logging.config
import yaml

debug = False
#debug = True
exp_score = 0

logging.config.dictConfig(yaml.load(open('../conf/logging.yaml', 'r')))
logger = logging.getLogger('kpi')

def run_cmd(cmd):
    global debug
    print(cmd)
    if debug:
        return 0
    else:
        res = os.system(cmd)
        return res

def loop(num, cmd, ctime):
    global exp_score
    try:
        res = run_cmd(cmd)
        if res == 0:
            logger.info('\033[91m' + '=========================================' + '\033[0m')
            logger.info(cmd + ' success!!')
            logger.info('loop' + str(num) + ' done takes: ' + str(time.time() - ctime) + 'seconds')
            logger.info('\033[91m' + '=========================================' + '\033[0m')
        else:
            raise Exception(cmd)
    except Exception as ex:
        exp_score += 1 << num
        logger.error('\033[91m' + '=========================================' + '\033[0m')
        logger.error("Exception:" + str(ex))
        logger.error(cmd + ' failed!!')
        logger.error('\033[91m' + '=========================================' + '\033[0m')

def main(t_day):
    global exp_score
    hadoop_conf = conf.get_conf('../conf/hadoop.conf', '=')
    hadoop_bin = hadoop_conf['HADOOP_BIN']
    pig_bin = hadoop_conf['PIG_BIN']
    hadoop_jar = hadoop_conf['HADOOP_JAR']
    hadoop_kpi = hadoop_conf['HADOOP_KPI']
    hadoop_log = hadoop_conf['HADOOP_LOG']
    hadoop_harlog = hadoop_conf['HADOOP_HARLOG']
    hadoop_log_file_name = hadoop_conf['HADOOP_LOG_FILE_NAME']

    
    input_path = hadoop_log + '/' + t_day
    output_path = hadoop_kpi + '/' + t_day + '/' + t_day
    harlog_name = t_day + '.har'
    harlog_path = hadoop_harlog + '/' + t_day + '.har' + '/part*'

    ##     delete the output_path
    cmd = hadoop_bin + ' fs -rm -r ' + output_path
    run_cmd(cmd)

    ##    hadoop archive 压缩
    cmd = hadoop_bin + ' archive -archiveName ' + harlog_name + ' -p ' + input_path + ' ' + hadoop_harlog
    try:
        if run_cmd(cmd) == 0:
            run_cmd(hadoop_bin + ' fs -rm -r ' + input_path)
        else:
            raise Exception(cmd)
    except Exception as ex:
        logger.error('hadoop archive error/exception occurred.')
        logger.error("Exception:" + str(ex))
        sys.exit(1)

    ##    execute the mapredcue jar
    cmd = hadoop_bin + ' jar ' + hadoop_jar + ' com.jobs.kpi.mapred.KPIMapRed ' + \
        harlog_path + ' ' +  output_path
    try:
        ctime = time.time()
        a = run_cmd(cmd)
        if a == 0:
            logger.info('\033[91m' + '=========================================' + '\033[0m')
            logger.info(cmd + ' success!!')
            logger.info(cmd  + ' done takes ' + str(time.time() - ctime) + 'seconds')
            logger.info('\033[91m' + '=========================================' + '\033[0m')
        else:
            raise Exception(cmd)
    except Exception as ex:
        exp_score += 1 << 0
        logger.error('\033[91m' + '=========================================' + '\033[0m')
        logger.error('\nSome error/exception occurred.')
        logger.error("Exception:" + str(ex))
        logger.error('\033[91m' + '=========================================' + '\033[0m')
        sys.exit(1)
    ##    Determine the hdfs file exists
    pu = run_cmd('hadoop fs -test -e ' + output_path + '/BasicField')
    if pu != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/BasicField')

    pt = run_cmd('hadoop fs -test -e ' + output_path + '/PageTimeLength')
    if pt != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/PageTimeLength')

    acc = run_cmd('hadoop fs -test -e ' + output_path + '/HeatMap')
    if acc != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/HeatMap')

    ads = run_cmd('hadoop fs -test -e ' + output_path + '/Ads')
    if ads != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/Ads')

    adspt = run_cmd('hadoop fs -test -e ' + output_path + '/AdsTimeLength')
    if adspt != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/AdsTimeLength')

    hot = run_cmd('hadoop fs -test -e ' + output_path + '/HotLink')
    if hot != 0:
        run_cmd('hadoop fs -mkdir -p ' + output_path + '/HotLink')

    #1
    domain_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                           ' -p time=' + t_day + ' kpi_domain_d.pig'
    #2
    page_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                         ' -p time=' + t_day + ' kpi_page_d.pig'
    #3
    source_analysis_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                                    ' -p time=' + t_day + ' kpi_source_analysis.pig'
    #4
    region_cmd = pig_bin + ' -p inputPU=' + output_path + '/BasicField' + ' -p inputPT=' + output_path + '/PageTimeLength' + \
                           ' -p time=' + t_day + ' kpi_region_distribute.pig'
    #5
    hot_cmd = pig_bin + ' -p inputHeat=' + output_path + '/HeatMap' + ' -p inputHotLink=' + output_path + '/HotLink' + \
                        ' -p time=' + t_day + ' kpi_heatmap_d.pig'
    #6
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
        logger.info('loop' + str(i+1) + ' is alive ' + str(threads[i].isAlive()))
        logger.info('loop' + str(i+1) + ' is daemon ' + str(threads[i].isDaemon()))

def is_valid_date(str):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str, "%Y%m%d")
        return True
    except:
        return False


if __name__ == '__main__':
    t_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    logger.info('\033[91m' + '==========================================' + t_day + '=====================================' + '\033[0m')
    ## 判断是否传参数
    if len(sys.argv) > 1: # 无传参，默认就是昨天
        #t_day = time.strftime('%Y%m%d',time.localtime(time.time()))
        t_day = str(sys.argv[1])
        if not is_valid_date(t_day):
            logger.error('\033[91m' + '请输入正确的日期格式(年月日), 如:' + time.strftime('%Y%m%d',time.localtime(time.time()))  + '\033[0m')
            sys.exit(-1)

    main(t_day)

    ##########################################################################################################################################
    logger.info('\033[91m' + '===============================================================================' + '\033[0m')
    logger.info('\033[91m==========================' + 'Exception Score is %d================================\033[0m'%exp_score)
    logger.info('\033[91m' + '===============================================================================' + '\033[0m\n\n\n\n')

