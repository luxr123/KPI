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
import multiprocessing
import logging
import logging.config
import yaml
import optparse

debug = False
#debug = True

logging.config.dictConfig(yaml.load(open('../conf/logging.yaml', 'r')))
logger = logging.getLogger('kpi')

class Worker(multiprocessing.Process):

    def __init__(self, cmd_queue, num, ctime):
        super().__init__()
        self.cmd_queue = cmd_queue
        self.num = num
        self.ctime = ctime

    def run(self):
        while True:
            try:
                cmd = self.cmd_queue.get()
                self.process(cmd)
            finally:
                self.cmd_queue.task_done()

    def process(self, cmd):
        try:
            res = run_cmd(cmd)
            if res == 0:
                logger.info('%s success!!'%cmd)
                logger.info('process {0} done takes: {1:.2f} seconds'.format(self.num, time.time() - self.ctime))
            else:
                raise Exception(cmd)
        except Exception as ex:
            logger.error("\033[91m Exception:%s\033[0m" %str(ex))
            logger.error('\033[91m{0:.<15}{1} failed!!\033[0m'.format(multiprocessing.current_process().name, cmd))

def run_cmd(cmd):
    global debug
    print(cmd)
    if debug:
        return 0
    else:
        res = os.system(cmd)
        return res

def main(t_day):
    hadoop_conf = conf.get_conf('../conf/hadoop.conf', '=')
    hadoop_bin = hadoop_conf['HADOOP_BIN']
    pig_bin = hadoop_conf['PIG_BIN']
    hadoop_jar = hadoop_conf['HADOOP_JAR']
    hadoop_kpi = hadoop_conf['HADOOP_KPI']
    hadoop_log = hadoop_conf['HADOOP_LOG']
    hadoop_harlog = hadoop_conf['HADOOP_HARLOG']

    
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
        logger.error('\033[91m hadoop archive error/exception occurred.\033[0m')
        logger.error("\033[91m Exception:%s\033[0m" %str(ex))
        sys.exit(1)

    ##    execute the mapredcue jar
    cmd = '{0} jar {1} com.jobs.kpi.mapred.KPIMapRed {2} {3}'.format(
                                            hadoop_bin, hadoop_jar, harlog_path, output_path)
    try:
        ctime = time.time()
        a = run_cmd(cmd)
        if a == 0:
            logger.info('%s success!!'%cmd)
            logger.info('{0} done takes: {1:.2f} seconds.'.format(cmd, time.time() - ctime))
        else:
            raise Exception(cmd)
    except Exception as ex:
        logger.error('\nSome error/exception occurred.')
        logger.error("\033[91m Exception:%s\033[0m" %str(ex))
        sys.exit(1)
    ##    Determine the hdfs file exists
    hdfs_outdirs = frozenset(['BasicField', 'PageTimeLength', 'Ads', 'AdsTimeLength', 'HotLink'])
    # 确认目录存在不管是否为空
    assert_outdir_exist(hadoop_bin, output_path, hdfs_outdirs)

    PIG_CMD_TEMPLATE1 = '{0} -p inputPU={1} -p inputPT={2} -p time={3} {{0}}'.format(
                    pig_bin, output_path+'/BasicField', output_path+'/PageTimeLength', t_day)
    PIG_CMD_TEMPLATE2 = '{0} -p inputHeat={1} -p inputHotLink={2} -p time={3} {{0}}'.format(
                    pig_bin, output_path+'/HeatMap', output_path+'/HotLink', t_day)
    PIG_CMD_TEMPLATE3 = '{0} -p inputAds={1} -p inputAdsPT={2} -p time={3} {{0}}'.format(
                    pig_bin, output_path+'/Ads', output_path+'/AdsTimeLength', t_day)

    domain_cmd = PIG_CMD_TEMPLATE1.format('kpi_domain_d.pig')
    page_cmd = PIG_CMD_TEMPLATE1.format('kpi_page_d.pig')
    source_analysis_cmd = PIG_CMD_TEMPLATE1.format('kpi_source_analysis.pig')
    region_cmd = PIG_CMD_TEMPLATE1.format('kpi_region_distribute.pig')
    hot_cmd = PIG_CMD_TEMPLATE2.format('kpi_heatmap_d.pig')
    ads_cmd = PIG_CMD_TEMPLATE3.format('kpi_ads_d.pig')
    
    loops_cmd = [domain_cmd, page_cmd, source_analysis_cmd, region_cmd, hot_cmd, ads_cmd]

    nloops = range(len(loops_cmd))
    
    cmd_queue = multiprocessing.JoinableQueue()
    for i in nloops:
        worker = Worker(cmd_queue, i+1, time.time())
        worker.daemon = True
        worker.start()

    for i in nloops:
        cmd_queue.put(loops_cmd[i])
    
    cmd_queue.join()

def assert_outdir_exist(hadoop, output_path, hdfs_outdirs):
    for dir in hdfs_outdirs:
        dir = output_path + '/' + dir
        cmd = '{0} fs -test -e {1}'.format(hadoop, dir)
        if run_cmd(cmd) != 0:
            run_cmd('{0} fs -mkdir -p {1}'.format(hadoop, dir))
    

def is_valid_date(str):
    '''判断是否是一个有效的日期字符串'''
    try:
        time.strptime(str, "%Y%m%d")
        return True
    except:
        return False


_TIP_INFO_TEMPLATE = ('\033[91m 请输入正确的日期格式(年月日)\n'
                      '如:\n'
                       'python3 {0} -d {1}\n'
                      'or\n'
                      'python3 {0} --date {1}\033[0m')

if __name__ == '__main__':
    start_time = time.time()
    t_day = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    parser = optparse.OptionParser()
    parser.add_option('-d', '--date', dest='date', type='string',
            help=('需要统计的日期(可选) [默认值: %default]'))
    parser.set_defaults(date=t_day)
    opts, args = parser.parse_args()
    t_day = opts.date
    
    if not is_valid_date(t_day):
        logger.error(_TIP_INFO_TEMPLATE.format(sys.argv[0], time.strftime('%Y%m%d',time.localtime(time.time()))))
        sys.exit(-1)

    logger.info('\033[91m {0:=^100} \033[0m'.format(t_day))

    main(t_day)

    logger.info('\033[91m 全程历时: {0:.2f} seconds.\033[0m\n\n\n\n'.format(time.time() - start_time))
