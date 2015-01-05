#!/usr/bin/env python
import os
import re
import sys
from threading import Thread
import threading
from Queue import Queue
from HTMLParser import HTMLParser
import MySQLdb
import time
import datetime
import pexpect

param={}
states=[]
states_shorts=[]
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()

def stripTags(s):
    ''' Strips HTML tags.
        Taken from http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/440481
    '''
    intag = [False]

    def chk(c):
        if intag[0]:
            intag[0] = (c != '>')
            return False
        elif c == '<':
            intag[0] = True
            return False
        return True

    return ''.join(c for c in s if chk(c))

def connectDB():
  db=cursor=''
  try:
    db = MySQLdb.connect(host=param['host'],user=param['user'],passwd=param['passwd'],db=param['database'])
    cursor = db.cursor()
  except Exception,e:
    print "Unsuccessfull DB connection"
    sys.exit(-1)
  return db,cursor

def get_patt(toks):
  dd=['St.','St','Street','Ave.','Avenue']
  for tok in dd:
    if tok in toks:
       return tok
  return 0

def readConf(confPath):
  fp=open(confPath)
  for line in fp:
    line=(line.strip("\n")).strip()
    toks=line.split("=")
    param[toks[0]]=toks[1]

def AddFilesToQueue(queue):
    files=os.listdir(param['home']+"/messages")
    threads=int(param['no_of_threads'])
    for file in files:
        queue.put(file)

    for i in range(0,threads):
        queue.put("QUIT")



def process_address(toks,index,lock,out,db,cursor):
  addr=''
  begin=end=flag=0
  if len(toks)>index+1:
    for i in range(index-1,-1,-1):
       if toks[i].isdigit():
         begin=i
         addr=toks[i]+" "+addr
         break
       else:
         addr=toks[i]+" "+addr
    for i in range(index,len(toks)):
      if toks[i].isdigit():
        end=i
        addr=addr+" "+toks[i]
        break
      else:
        addr=addr+" "+toks[i]
  else:
    index=index-1
    ff=flag=0
    for i in range(index,-1,-1):
      if toks[i].isdigit() and ff==0:
         flag=1
         end=i
         addr=toks[i]
      elif toks[i].isdigit() and ff==1:
        begin=1
        addr=toks[i]+" "+addr
        break
      else:
        if flag==1:
          ff=1
          addr=toks[i]+" "+addr
  sql="insert into postal_addresses values('"+addr+"',"+str(int(time.time()))+","+str(int(time.time()))+","+str(1)+",'"+param['type']+"');"
  try:
     cursor.execute(sql)
     pass
  except Exception,e:
     sql="update postal_addresses set updation="+str(int(time.time()))+",count=count+1 where (address='"+addr+"' AND info='"+param['type']+"');" 
     try:
        cursor.execute(sql)
        pass
     except Exception,e:
        print e,sql
  lock.acquire()
  out.write(addr+"\n")
  lock.release()

def handle_excep(addr,fail,lock):
   #print addr
   if "Address:" in addr:
      addr=addr.split("Address:")[-1]
      toks=addr.split(" ")
      for tok in toks:
        if len(tok)>=2 and (tok in states or tok in states_shorts):
           index=toks.index(tok)
           return toks,index,tok
        else:
           lock.acquire()
           fail.write(addr+"\n")
           lock.release()
   else:
     lock.acquire()
     fail.write(addr+"\n")
     lock.release()
   return 0,0,0

def extract_address(fname,lock,out,fail,db,cursor):
  fp=open(param['home']+"/messages/"+fname)
  #msg=fp.read()
  flag=0
  new_msg=''
  for line in fp:
    if (line=="\r\n" or line =="\n"):
      flag=1
      continue
    if flag==1:
      new_msg=new_msg+line
  msg=new_msg

  msg=stripTags(msg)
  #pp=msg.split("\n")
  msg=(((((msg.replace("\n"," ")).replace(",","")).replace("=","")).replace("\t","")).replace("-","")).replace("|","")
  msg=re.sub(' +',' ',msg)
  #Pb=re.findall('(([P|p][.]?[ ]?[O|o][.]?[ ]?)([B|b][O|o][X|x])?)|(Post Office Box)|([B|b][O|o][X|x])',msg)
  #stNo=re.findall('([0-9]{1,6}|One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten)(\w+\s+){1,10}',msg) 
  #stNo=re.findall('([0-9]{1,6}(#?\w+.?\s+){5,15}((([P|p][.]?[ ]?[O|o][.]?[ ]?)([B|b][O|o][X|x])?)|(Post Office Box)|([B|b][O|o][X|x]))?[0-9]{5}([- ][0-9]{4})?(\s+?\w+){1,2})',msg)
  #stNo=re.findall('([0-9]{1,6}(#?\w+.?\s+){5,15}[0-9]{5}([- ][0-9]{4})?(\s+?\w+){1,2})',msg)
  stNo=re.findall('[0-9]{1,6}',msg)
  prev_index=cur_index=prev=-1
  substr=raw_string=''
  for i in range(len(stNo)):
    if i==0 or prev_index==-1:
      prev_index=msg.find(str(stNo[i]))
      prev=stNo[i]
    else:
      cur_index=msg.find(str(stNo[i]),prev_index+1)
      substr=msg[prev_index:cur_index]
      substr=substr+str(stNo[i])
      raw_string=(substr.split(stNo[i-1])[-1]).split(stNo[i])[0]
      
      prev_index=cur_index
    if len(raw_string)>0 and len(substr)<50:
      #print substr
      #toks=re.split(delim,raw_string)
      #toks=filter(None, toks)
      if len(raw_string.split("/"))>3 or len(raw_string)>50:
         continue
      if ('(' in raw_string and ')' in raw_string):
         continue
      toks=raw_string.split(" ")
      if len(toks)>3 and len(toks)<10:
        #print raw_string
  #if len(stNo)==0 or len(stNo)>2:
  #  return 0
  #print stNo
        data=re.search("[St\.|Street|Ave\.|Avenue|Dept\.|University|Corp\.|Corporations?|College|Laboratory|[D|d]isclaimer|Division|Professor|Laboratories|Institutes?|Services|Engineering|Director|Road|East|West|Sciences?|Manager|South|North].*",substr)
        if data!=None:
           patt=data.group(0)
           #print dd[0],patt
           toks=substr.split(" ")
           pt=get_patt(toks)
           if pt!=0:
             patt=pt
           index=0
           try:
            index=toks.index(patt)
           except:
            d=re.search(patt+"[a-zA-Z]+",substr)
            if d!=None:
              try:
                index=toks.index(str(d.group(0)))
              except:
                lock.acquire()
                fail.write(substr+"\n")
                lock.release()
                continue 
            else:
              toks,index,patt=handle_excep(substr,fail,lock)
              #print patt,index,toks
              if toks==0 or index==0 or patt==0 :
                lock.acquire()
                fail.write(substr+"\n")
                lock.release()
                continue
           process_address(toks,index,lock,out,db,cursor)
        else:
          toks=substr.split(" ")
          ff=0
          for tok in toks:
            if len(tok)>=2 and (tok in states or tok in states_shorts):
              index=toks.index(tok)
              ff=1
              process_address(toks,index,lock,out,db,cursor)
          if ff==0:
             lock.acquire()
             fail.write(substr+"\n")
             lock.release()

def process_files(queue,j,out,fail):
  lock=threading.Lock()
  count=0
  db,cursor=connectDB()
  while True:
     fname=queue.get()
     count=count+1
     print "Thread"+str(j)+" processing "+ str(fname)+"count="+str(count)
     if fname != "QUIT":
        extract_address(fname,lock,out,fail,db,cursor)
     else:
        print "Thread"+str(j)+" quitting..."
        db.commit()
        db.close()
        break

def main():
  var=''
  if len(sys.argv) !=2:
    var = raw_input("Enter conf complete path: ")
  else:
    var=sys.argv[1]
  readConf(var)
  os.chdir(param['home'])
  cmd="rm -rf "+param['home']+"/messages"
  os.system(cmd)
  day=str(datetime.date.fromordinal(datetime.date.today().toordinal()-2))
  day=day.replace("-","")
  fp=open(param['home']+"/states")
  for line in fp:
    line=line.strip("\n")
    toks=line.split(",")
    states.append(toks[0].strip())
    states_shorts.append(toks[1].strip())
  fp.close()

  for i in range(2):
    cmd=''
    if i==0:
      cmd="scp idev01@www46.mailshell.com:/mnt/archive/"+param['type']+"/missed_processed/missed-"+day+".tgz "+param['home']+"/"
    else:
      cmd="scp idev01@www46.mailshell.com:/mnt/archive/"+param['type']+"/missed_processed/caught-"+day+".tgz "+param['home']+"/"
    child = pexpect.spawn(cmd,timeout=None)
    child.expect('.*password.*')
    child.sendline('we@590009')
    child.expect(pexpect.EOF)
    child.close(force=True)
    cmd=''
    if i==0:
       cmd="tar xzf "+param['home']+"/missed-"+day+".tgz"
       #print cmd
       os.system(cmd)
       cmd="mv "+param['home']+"/missed-"+day+" "+param['home']+"/messages/"
       os.system(cmd)
    else:
       cmd="tar xzf "+param['home']+"/caught-"+day+".tgz"
       #print cmd
       os.system(cmd)
       cmd="cp "+param['home']+"/caught-"+day+"/* "+param['home']+"/messages/"
       os.system(cmd)
 
  cmd="rm -rf "+param['home']+"/caught-"+day
  os.system(cmd)
  os.system("rm "+param['home']+"/caught-"+day+".tgz")
  cmd="rm -rf "+param['home']+"/missed-"+day
  os.system(cmd)
  os.system("rm "+param['home']+"/missed-"+day+".tgz")

  out=open(param['home']+"/"+param['type']+"_out.txt","w")
  fail=open(param['home']+"/"+param['type']+"_fail.txt","w")
  pthreads = []
  
  queue = Queue()
  
  thread=Thread(target=AddFilesToQueue,args=(queue,))
  thread.Daemon = True
  thread.start()
  pthreads.append(thread)
  
  cthreads =[]
  
  for j in range(int(param['no_of_threads'])):
    thread=Thread(target=process_files,args=(queue,j,out,fail))
    thread.daemon = True
    thread.start()
    cthreads.append(thread)

  pthreads[0].join()
  
  for ct in cthreads:
     ct.join()  
  out.close()
  fail.close()
  print "\nRemovin all files\n"
  cmd="rm -rf "+param['home']+"/messages"
  os.system(cmd)
  
if __name__=="__main__":
  main()
