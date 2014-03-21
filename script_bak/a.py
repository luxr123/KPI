#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os,sys
cmd = 'hadoop fs -test -e /kpi/20131129/2013112911/PW_*'
a = os.system(cmd)
if a != 0:
    print('/kpi/20131129/2013112911/PW_*' + ' not exist !!')
    os.system('hadoop fs -mkdir -p /kpi/20131129/2013112911/PW_')
    






