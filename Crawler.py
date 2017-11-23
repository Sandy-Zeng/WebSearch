# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import requests
import re
import threading
import cPickle
import os
import time
import sqlite3
from bs4 import BeautifulSoup
from requests import exceptions
from collections import deque

def writeData(bp,path,data):
    try:
        if os.path.exists(bp):
            f = open(path, 'w')
            f.write(data)
            f.close()
    except (OSError, WindowsError, IOError) as e:
        print str(e.message)

#log the error message
def log(url,reson,message):
    errortime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()));
    try:
        f = open("Log.txt", 'w')
        f.write()
        f.write(errortime+'\n')
        f.write(url+'\n')
        f.write(reson+'\n')
        f.write(message+'\n')
        f.close()
    except (OSError, WindowsError, IOError) as e:
        print str(e.message)

def createTable():
    conn = sqlite3.connect('spider.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE webpage
                (
                 pageID INTEGER PRIMARY KEY  NOT NULL,
                 url TEXT NOT NULL UNIQUE ,
                 title TEXT,
                 body TEXT NOT NULL
                )''')
    c.execute('''CREATE TABLE link
                 (
                   linkID INTEGER PRIMARY KEY NOT NULL,
                   sourceURL TEXT NOT NULL ,
                   dstURL TEXT NOT NULL,
                   anchor TEXT
                 )''')
    conn.close()
    print "opened successfully"

def insertpage(url,title,content):
    conn = sqlite3.connect('spider.db')
    c = conn.cursor()
    try:
        c.execute("INSERT INTO webpage(pageID,url,title,body) VALUES (NULL,?,?,?)",(url,title,content))
        conn.commit()
    except exceptions as e:
        print "插入错误："+str(e.message)
        log(url,"插入错误",str(e.message))
    print 'insert successfully'
    conn.close()

def insertRelation(sourceurl,dsturl,anchor,c):
    try:
        c.execute("INSERT INTO link(linkID,sourceURL,dstURL,anchor) VALUES (NULL,?, ?, ?)",(sourceurl,dsturl,anchor))
    except exceptions as e:
        print "插入错误：" + str(e.message)
        log(sourceurl, "插入错误", str(e.message))

#请求网页文本并保存到文件
def download(url,count):
    try:
        type = str(url).split(".")
        suffixe = type[len(type)-1]
        #if suffixe not in suffixes:return ""
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        header = {'Connection': "close",'User-Agent':user_agent}
        r = requests.get(url,headers=header,timeout=5)
        encoding = 'utf-8'
        if r.status_code == 200 and suffixe in media:
            basePath = 'webInfo/' + str(count)
            if os.path.exists(basePath) == False:
                os.mkdir(basePath)
            basePath = 'webInfo/' + str(count) +"/"+str(count)+"."+ suffixe
            if (basePath != ""):
                with open(basePath, 'wb') as f:
                    for chunk in r:
                        f.write(chunk)
                return ""
        if r.status_code == 200:
            soup = BeautifulSoup(r.text, 'html.parser')
            body = (soup.text.encode(encoding, 'ignore').decode(encoding))
            title = soup.find_all('title')
            '''if len(title)==0:
                basePath = 'WebPage/' + str(count)
            else:
                basePath = 'WebPage/' + str(count) + title[0].get_text().encode(encoding, 'ignore').decode(encoding)
            if os.path.exists(basePath) == False:
                try:
                    os.makedirs(basePath)
                except (OSError,WindowsError,IOError) as e:
                    print str(e.message)
                    log(url,"IOERROR",str(e.message))'''
            basePath = 'webInfo'
            if suffixe not in webpage:
                suffixe = "html"
            #record relevant message
            writeData(basePath,basePath + '/page.'+suffixe, r.text)
            #insert info of the page to db
            if len(title)!=0:
                insertpage(url,title[0].get_text(),body)
            else:
                insertpage(url,"notitle",body)
        return r.text
    except exceptions.Timeout as e:
        print "请求超时"+str(e.message)
        log(url,"请求超时",str(e.message))
        return ""
    except exceptions.HTTPError as e:
        print "http请求错误："+str(e.message)
        log(url, "http请求错误", str(e.message))
        return ""
    except exceptions.ConnectionError as e:
        print "链接失效"+str(e.message)
        log(url, "链接失效", str(e.message))
        return  ""
    except exceptions.InvalidSchema as e:
        print "无匹配" + str(e.message)
        log(url, "无匹配", str(e.message))
        return ""

def serializeSet(urlSet):
    f = open('AlreadyGetURL.txt', 'wb')
    cPickle.dump(urlSet,f)
    f.close()

def serializeQueue(queue):
    f = open('NotGet.txt','wb')
    cPickle.dump(queue,f)
    f.close()

def serializeCount(count):
    f = open('Count.txt','wb')
    cPickle.dump(count,f)
    f.close()

def serializeList(list):
    f = open('BlackList.txt','wb')
    cPickle.dump(list,f)
    f.close()

def fun_timer():
    global timer
    time.sleep(sleepTime)
    timer = threading.Timer(runTime,fun_timer)
    timer.start()

def fun_timer2():
    global timer2
    #recort the info
    serializeSet(urlSet)
    serializeQueue(urlQueue)
    serializeCount(count)
    serializeList(blackList)
    timer2 = threading.Timer(writeTime, fun_timer2)
    timer2.start()

#initialize before spider
def initialize():
    global urlSet,urlQueue,count,blackList
    if os.path.getsize('AlreadyGetURL.txt'):
        f = open('AlreadyGetURL.txt', 'rb')
        urlSet = cPickle.load(f)
    if os.path.getsize('NotGet.txt'):
        f = open('NotGet.txt', 'rb')
        urlQueue = cPickle.load(f)
    if os.path.getsize('Count.txt'):
        f = open('Count.txt', 'rb')
        count = cPickle.load(f)
    if os.path.getsize('BlackList.txt'):
        f = open('BlackList.txt', 'rb')
        blackList = cPickle.load(f)

#get the url and anchor in this webpage
def praser(sourceurl,page):
    soup = BeautifulSoup(page, 'html.parser')
    tags = soup.find_all('a', attrs={"href": re.compile(r'^http:')})
    conn = sqlite3.connect('spider.db')
    c = conn.cursor()
    for tag in tags:
        url = str(tag['href'])
        anchor = tag.get_text()
        #判断这个网页是否被爬取过
        if url not in urlSet and url not in urlQueue and ("nankai" in str(url) or 'nk' in str(url)):
            urlQueue.append(url)
        #insert the relation to db
        insertRelation(sourceurl, url, anchor, c)
    conn.commit()
    conn.close()

def checkType(type):
    for acceptType in mintype:
        if acceptType in type:
            return True
    return False


#main function
def spider():
    global urlQueue
    while len(urlQueue)!=0:
        url = urlQueue.popleft()
        # not in blackList
        domain = re.findall(r"(?<=://)[a-zA-Z\.0-9]+(?=\/)", url)
        if len(domain)==0:
            domain = re.findall(r"(?<=://)[a-zA-Z\.0-9]+", url)
        print url
        if len(domain)==0:continue
        if blackList.__contains__(domain[0]) == False:
            text = ""
            try:
                header = {"Connection":"close"}
                #get headers of the request
                text = requests.get(url,headers=header,timeout=5).headers
            except exceptions.Timeout as e:
                print "请求超时" + str(e.message)
                log(url,"请求超时",str(e.message))
            except exceptions.HTTPError as e:
                print "http请求错误：" + str(e.message)
                log(url,"http请求错误：" ,str(e.message))
            except exceptions.ConnectionError as e:
                print "链接失效" + str(e.message)
                blackList.append(url)# add the url to blackList
                log(url,"链接失效" , str(e.message))
            except exceptions.InvalidSchema as e:
                print "无匹配"+str(e.message)
                blackList.append(url)
                log(url, "无匹配", str(e.message))

            #check the Content-Type
            if text.__contains__('Content-Type'):
                type = text['Content-Type']
                print  type
                #examine type of response
                if checkType(str(type)):
                    global count
                    response = download(url,count)
                    print url
                    count = count + 1
                    urlSet.add(url)
                    #response not none
                    if response.strip()!="":
                        praser(url,response)
        time.sleep(0.5)

#urlSet = loadURLSet()
urlQueue = deque()
count = 0
urlSet = set()
blackList = ["192.168.129.51:8080"]
initialize()

anchorList = list()
if len(urlSet)==0:
    urlSet.add("http://www.nankai.edu.cn/") #set the source url
if len(urlQueue)==0:
    urlQueue.append("http://www.nankai.edu.cn/")
if count <= 1:
    count = 1

suffixes = set(["html","jpg","png","php","jsp","jpeg","mp3","mp4","pdf","txt","cn","cn/","gif","doc","avi","mpeg","wav","exe","rar","zip","shtml","htm","ppt"])
media = set(["jpg","png","jpeg","mp3","mp4","pdf","gif","avi","mpeg","wav","exe","rar","zip","ppt"])
webpage = set(["html","shtml","htm","php","jsp","asp"])
mintype = set(["text/html","image/jpeg","video/mpeg","text/xml","audio/wav","application/pdf","application/zip","application/x-png","application/x-ppt","audio/mp4","image/png"])

#set the timer
sleepTime = 60
runTime = 300
writeTime = 5
timer = threading.Timer(runTime,fun_timer)
timer.start()
timer2 = threading.Timer(writeTime,fun_timer2)
timer2.start()

createTable()

#begin spider
spider()