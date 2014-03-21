#!/bin/bash

set -x

export JAVA_HOME=/usr/local/jdk
source /home/hadoop/.bash_profile
export LANG="en_US.UTF-8"
export LC_ALL="en_US.UTF-8"

/home/hadoop/lu/pig-0.10.1/bin/pig -p inputPU=/kpi/20140303/20140303/BasicField -p inputPT=/kpi/20140303/20140303/PageTimeLength -p time=20140213 kpi_region_distribute.pig
