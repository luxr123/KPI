#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 5068    Tuocheng County 柘城县
import sys,os
import string
if __name__ == '__main__':
	f = open(sys.argv[1])
	lines = f.readlines()
	f.close()
	for line in lines:
		items = line.strip().split('\t')
		col1 = items[0].strip()
		col2 = items[1].strip()
		col3 = items[2].strip()
		items1 = col2.split(' ')
		name = items1[0].strip().lower()
		print col1 + '\t' + name + '\t' + col3
		
