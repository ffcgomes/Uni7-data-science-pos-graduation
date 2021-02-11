#!/usr/bin/env python2.7

import json
from multiprocessing import Pool
from multiprocessing import cpu_count
import requests
import signal,sys,time
import psutil
import os
import shutil
import datetime

TERMINATE=False
INSTANCEID=''
ACCOUNTID=''
STRESS=9999
STATUSHTML='/var/www/html/prime.html'
VERSION='1.0.0.5'
SERVER='https://prime-server-uni7.herokuapp.com'

def signal_handling(signum,frame):
    global TERMINATE
    TERMINATE=True

def getpublicip():    
  global INSTANCEID
  URL="http://169.254.169.254/latest/meta-data/instance-id"
  try:
    r=requests.get(url=URL, timeout=5)
    INSTANCEID=r.text
  except:
    print 'Falha ao recuperar meta-data instance-id :('

def getaccountid():
  global ACCOUNTID
  URL="http://169.254.169.254/latest/meta-data/identity-credentials/ec2/info"
  try:
    r=requests.get(url=URL, timeout=5)
    ACCOUNTID=r.json()['AccountId']
  except:
    print 'Falha ao recuperar meta-data AccountId :('
    
def statusupdate(prime):    
  global SERVER
  global VERSION
  global STRESS
  global INSTANCEID
  global ACCOUNTID
  global STATUSHTML
  # f=open(STATUSHTML, "w+", buffering=0)
  with open(STATUSHTML+".tmp", "w+") as f:
    html=""
    html=html+"<html>"
    html=html+"<header>"
    html=html+"<title>"+ACCOUNTID+"</title>"
    html=html+"<meta http-equiv='refresh' content='2'>"
    html=html+"</header>"
    html=html+"<body>"
    html=html+"<h1>Status nessa instancia ("+ACCOUNTID+" | "+INSTANCEID+")</h1><br />"
    html=html+"<p>instance-id ("+ACCOUNTID+"): "+INSTANCEID+"</p><br />"
    html=html+"<p>datetime: "+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"</p><br />"
    html=html+"<p>primo: "+str(prime)+"</p><br />"
    html=html+"</body>"
    html=html+"</html>"
    f.write(html)
    f.flush()
    os.rename(STATUSHTML+".tmp", STATUSHTML)
    
def lastprime():
  global SERVER
  global VERSION
  URL=SERVER+"/lastprimo"
  PARAMS={'name': ACCOUNTID}
  try:
    r=requests.get(url=URL, params=PARAMS, timeout=5)
    print 'last prime received: ' + r.text
    return int(r.text)
  except:
    print 'Falha na comunicacao com o servidor '+SERVER+' :('
    time.sleep(30)
    return int(0)

def primofound(prime):
  global SERVER
  global VERSION
  URL=SERVER+"/primofound"
  PARAMS={'number': prime, 'name': ACCOUNTID+'_'+INSTANCEID}
  try:
    r=requests.get(url=URL, params=PARAMS, timeout=5)
    print 'next prime delivered: ' + str(prime)
  except:
    print 'Falha na comunicacao com o servidor '+SERVER+' :('
    time.sleep(30)

def f(x):
  signal.signal(signal.SIGINT,signal_handling)
  global STRESS
  for j in range(0, x+STRESS):
    if TERMINATE:
      break
    else:
      x*x

def main():
  signal.signal(signal.SIGINT,signal_handling)
  global SERVER
  global VERSION
  print 'Iniciando (version '+VERSION+')...'
  print 'Server: '+SERVER
  statusupdate('iniciando...')
  getpublicip()
  getaccountid()
  while True:
    LAST=lastprime()
    if (LAST > 0):
      statusupdate(LAST+1)
      primofound(find_next_prime(LAST+1))
    if TERMINATE:
      print "Interrompido por sinal!"
      break
  print "Bye"
  sys.exit()

def find_next_prime(n):
    if n < 1:
      n=1
    return find_prime_in_range(n, 2*n)

def find_prime_in_range(a, b):
    for p in range(a, b):
        for i in range(2, p):
            if p % i == 0:
                pool.map(f, range(p))
                # print('Nao primo: ', str(p))
                break
        else:
            return p
    return None

if __name__ == '__main__':
  processes=cpu_count()
  pool=Pool(processes)
  main()
