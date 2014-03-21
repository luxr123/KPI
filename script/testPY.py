#!/usr/bin/env python
# -*- coding: utf-8 -*-

import threading
from time import sleep, time

loops = [20,10]

def loop(nloop,nsec,a):
    print('start loop', nloop, 'at :',(int(time())-a),'秒')
    sleep(nsec)
    print('loop',nloop,'done at:',(int(time())-a),'秒')

def main():
    ztime=int(time())
    print('start at:', (int(time()-ztime)), '秒')
    threads = []
    nloops= range(len(loops))

    for i in nloops:
        t = threading.Thread(target=loop, args=(i,loops[i],ztime))
        threads.append(t)

    for i in nloops:#开始线程
        threads[i].setDaemon(True)
        threads[i].start()   

    for i in nloops:        #等待所有的子线程执行结束(最多等待3秒,默认为None，无限等待)
        threads[i].join(timeout=None)
        print('loop',i,'is alive',threads[i].isAlive())
        print('loop',i,'is daemon',threads[i].isDaemon())    

    print('all done at:',(int(time()-ztime)),'秒')

if __name__ == '__main__':
    main()
