#!//bin/env python
#ssh_cmd_ver2.py
#coding:utf-8
import pexpect
import os, sys, string, time, datetime, traceback
from multiprocessing import Process
 
cmds= '''date -s 19:59:00'''
 
def ssh_cmd(ip,port,user,keyfile,passwd,cmd):
    if keyfile != '':
        ssh = pexpect.spawn('ssh -p%s -i %s %s@%s "%s"' % (port,keyfile, user, ip, cmd))
 
        try:
            i = ssh.expect(["Enter passphrase for key '"+keyfile+"': ", 'continue connecting (yes/no)?'],timeout=60)
            if i == 0 :
                ssh.sendline(passwd)
                r = ssh.read()
            elif i == 1:
               ssh.sendline('yes\n')
               ssh.expect("Enter passphrase for key '"+keyfile+"': ")
               ssh.sendline(passwd)
               r = ssh.read()
        except pexpect.EOF:
            ssh.close()
            r=ip+":EOF"
        except pexpect.TIMEOUT:
            #ssh.close()
            r="ip:TIMEOUT"
        return r
 
    else:
        ssh = pexpect.spawn('ssh -p%s %s@%s "%s"' % (port, user, ip, cmd))
        try:
            i = ssh.expect(['password: ', 'continue connecting (yes/no)?'],timeout=60)
            if i == 0 :
                ssh.sendline(passwd)
                r = ssh.read()
            elif i == 1:
                ssh.sendline('yes\n')
                ssh.expect('password: ')
                ssh.sendline(passwd)
                r = ssh.read()
        except pexpect.EOF:
            ssh.close()
            r="EOF"
        except pexpect.TIMEOUT:
            #ssh.close()
            r="TIMEOUT"
        return r
 
def job_task(ip,port,user,keyfile,passwd):
    for cmd in cmds.split(","):
        r=ssh_cmd(ip,port,user,keyfile,passwd,cmd)
        print(r)
 
def main():
    print(("%s: controller started." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),)));
    hosts = open('./root.list');
    plist = []
    for host in hosts:
        if host:
            ip,port,user,keyfile,passwd = host.split(":")
            p = Process(target = job_task, args = (ip,port,user,keyfile,passwd))
            plist.append(p)
            #print plist
            p.start();
    for p in plist:
        p.join();
    print(("%s: controller finished." % (time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()),)))
 
if __name__=='__main__':
    main()
