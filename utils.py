
# coding: utf-8

# In[151]:

import requests
import urllib2
import json
import sqlite3
import sys
import os
import time
import numpy as np
import cv2
from PIL import Image
import logging

# In[63]:

url_format = format('http://2sc.api.che168.com/car/GetCarBrandSeriesfirstPicByTime.ashx?_appid={}sc&flag={}&btime={}&&etime={}')


# In[164]:

url = url_format.format(2,1, '2017-6-19__20_00_00', '2017-6-20__00_00_00')

format_2sc = "%Y-%m-%d__%H_%M_%S"
format_db = "%Y-%m-%d %H:%M:%S"


# In[205]:

#获取二手车某段时间内新增获取全部数据，并插入数据库
def fetch_new_and_insert(url_format, start_time, end_time, database, tag='new', logging=None):
    if tag == 'new':
        url = url_format.format(2,1, start_time, end_time)
    elif tag == 'all':
        url = url_format.format(2,3, start_time, end_time)

    result = {}
    result['returncode'] = -1
    #fetch new data
    try:
        if logging:
            logging.info('[fetch_new_and_insert] fetech new data from autohome_2sc, url:{}\n'.format(url))
        result = urllib2.urlopen(url).read()
        result = json.loads(result)
    except Exception,e:
        if logging:
            logging.info('[fetch_new_and_insert] fetech new data from autohome_2sc, error:{}'.format(e))  
        return -1  

    if result['returncode'] == 1000:
        messages = eval(result['message'])
        if type(messages) is dict:
            mess = []
            mess.append(messages)
            messages = tuple(mess)

    else:
        return -1

    
    start_time = time.strftime(format_db, time.strptime(start_time, format_2sc))
    end_time = time.strftime(format_db, time.strptime(end_time, format_2sc))
    #将查询操作记录在数据库中
    logdb_conn = sqlite3.connect(database)
    if logdb_conn is not None:
        logdb_cur = logdb_conn.cursor()
        logdb_insert = """insert into visit (id, operator, tag, source, start_time, end_time, count, add_time) values ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}')"""
        if tag == 'new':
            tag_mark = 1
        elif tag == 'all':
            tag_mark = 3
        else:
            tag_mark = -1
        cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())        
        logdb_comm = logdb_insert.format(str(time.time()), 'insert', tag_mark, 'autohome_2sc', start_time, end_time, len(messages), cur_time)
        #print logdb_comm
        if logging:
            logging.info(str('[fetch_new_and_insert] ')+logdb_comm)
        try:
            logdb_cur.execute(logdb_comm)
            logdb_conn.commit()
        except Exception,e:
            if logging:
                logging.warning('[fetch_new_and_insert] insert into visit, error:{}\n'.format(e))
        logdb_cur.close()
        logdb_conn.close()

    #将查询结果插入本地数据库
    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        #print 'messages = ', messages
        for index,mesg in enumerate(messages):
            inforid = mesg['infoid']
            brandid = mesg['brandid']
            brandname = mesg['brandname']
            seriesid = mesg['seriesid']
            seriesname = mesg['seriesname']
            url = mesg['sourcepic']
            insert_sql = """insert into autohome (infoid, brandid, brandname, seriesid, seriesname, url, add_time) values ('{}', {}, '{}', {}, '{}', '{}', '{}')"""
          
            try:
                cur_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                comm = insert_sql.format(str(inforid), brandid, brandname, seriesid, seriesname, url, cur_time)
                #print index,'|',len(message),':',comm
                if logging:
                    logging.debug(str('[fetch_new_and_insert] ')+comm)                

                cursor.execute(comm)
                conn.commit()                   
            except Exception,e:
                if logging:
                    logging.warning('[fetch_new_and_insert] insert into autohome, error:{}\n'.format(e))
        cursor.close()
        conn.close()

    #下载图像并生成code更新到数据库
    update_code(database, messages, logging)   


    return 0


#获取二手车某时间段内下架的数据，并从本地数据库中删除
def fetch_and_delete(url_format, start_time, end_time, database, logging=None):
    url = url_format.format(2,2, start_time, end_time)

    result = {}
    result['returncode'] = -1
    try:
        if logging:
            logging.info('[fetch_and_delete] fetch delete data, url:{}\n'.format(url))
        result = urllib2.urlopen(url).read()
        result = json.loads(result)
    except Exception,e:
        if logging:
            logging.info('[fetch_and_delete] fetch delete data, error:{}\n'.format(e))

    if result['returncode'] == 1000:
        messages = eval(result['message'])
    else:
        return -1
    
    start_time = time.strftime(format_db, time.strptime(start_time, format_2sc))
    end_time = time.strftime(format_db, time.strptime(end_time, format_2sc))
    #将查询操作记录在数据库中
    logdb_conn = sqlite3.connect(database)
    if logdb_conn is not None:
        logdb_cur = logdb_conn.cursor()
        logdb_insert = """insert into visit (id, operator, tag, source, start_time, end_time, count, add_time) values ('{}', '{}', {}, '{}', '{}', '{}', {}, '{}')"""

        cur_time = time.strftime(format_db, time.localtime())        
        logdb_comm = logdb_insert.format(str(time.time()), 'delete', 2, 'autohome_2sc', start_time, end_time, len(messages), cur_time)
        #print logdb_comm
        if logging:
            logging.debug(str('[fetch_and_delete] ')+logdb_comm) 

        try:
            logdb_cur.execute(logdb_comm)
            logdb_conn.commit()
        except Exception,e:
            if logging:
                logging.warning('[fetch_and_delete] insert int visit, error:{}\n'.format(e))
        logdb_cur.close()
        logdb_conn.close()


    #根据查询结果，删除本地数据库数据
    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        for mesg in messages:
            #print mesg
            infoid = mesg['infoid']
            brandid = mesg['brandid']
            brandname = mesg['brandname']
            seriesid = mesg['seriesid']
            seriesname = mesg['seriesname']
            url = mesg['sourcepic']
            insert_sql = """delete from autohome where infoid = '{}'
                """
            try:
                comm = insert_sql.format(str(infoid))
                #print comm
                if logging:
                    logging.debug(str('[fetch_and_delete] ')+comm) 
                cursor.execute(comm)
                conn.commit()
                #删除本地图像
                output_file = os.path.basename(url)
                output_file = os.path.join('./autohome_pic', output_file)
                #print 'rm ', output_file
                os.system('rm {}'.format(output_file))
                if logging:
                    logging.debug('[fetch_and_delete] rm {}'.format(output_file))

            except Exception,e:
                if logging:
                    logging.warning('[fetch_and_delete] delete from autohome, error:{}\n'.format(e))
        cursor.close()
        conn.close()
        
#加载csv文件数据
def load_autohome(filename):
    autohome_list = []
    with open(filename, 'r') as f:
        while 1:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            lines = line.split(",")
            auto = dict()
            auto['infoid'] = lines[0]
            auto['brandid'] = lines[1]
            auto['brandname'] = lines[2]
            auto['seriesid'] = lines[3]
            auto['seriesname'] = lines[4]
            auto['sourcepic'] = lines[5]
            autohome_list.append(auto)
    print 'autohome lens = ', len(autohome_list)
    return autohome_list        

#加载csv文件数据，并插入到本地数据库
def load_and_setup(database, filename, logging=None):
    
    #加载本地cvs文件数据
    messages = load_autohome(filename)

    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        for mesg in messages:
            inforid = mesg['infoid']
            brandid = mesg['brandid']
            brandname = mesg['brandname']
            seriesid = mesg['seriesid']
            seriesname = mesg['seriesname']
            url = mesg['sourcepic']
            insert_sql = """insert into autohome (infoid, brandid, brandname, seriesid, seriesname, url, add_time) values ('{}', {}, '{}', {}, '{}', '{}', '{}')
                """
            try:
                cur_time = time.strftime(format_db, time.localtime())
                comm = insert_sql.format(str(inforid), brandid, brandname, seriesid, seriesname, url, cur_time)
                #print comm
                if logging:
                    logging.debug(str('[load_and_setup] ')+comm)
                cursor.execute(comm)
                conn.commit()
            except Exception,e:
                if logging:
                    logging.warning('[load_and_setup] insert int autohome, error:{}\n'.format(e))
        cursor.close()
        conn.close()
    else:
        if logging:
            logging.warning('[load_and_setup] can not connect to database, error!!!')
        
#查询本地数据库，找出所有code1字段为空的数据
def lookup_null(database, logging=None):
    autohome_list = []
    
    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        select_sql = 'select * from autohome where code1 is NULL'
        try:
            if logging:
                logging.info(str('[lookup_null] ')+select_sql)
            cursor.execute(select_sql)
            result = cursor.fetchall()
        except Exception,e:
            if logging:
                logging.warning('[lookup_null] select * from autohome, error:{}\n'.format(e))
        cursor.close()
        conn.close()
        
        for mesg in result:
            auto = {}
            auto['infoid'] = mesg[0]
            auto['brandid'] = mesg[1]
            auto['brandname'] = mesg[2]
            auto['seriesid'] = mesg[3]
            auto['seriesname'] = mesg[4]
            auto['sourcepic'] = mesg[5]
            auto['add_time'] = mesg[10]
            #print auto['infoid'], auto['brandid'], auto['brandname'], auto['seriesid'], auto['seriesname'], auto['url'], auto['add_time']
            autohome_list.append(auto)
    
    update_code(database, autohome_list[:5000], logging)

    return autohome_list

#根据url下载图像数据
def load_imgFromUrl(url, logging=None):
    img = []
    try:
        if logging:
            logging.info('[load_imgFromUrl] load url:{}\n'.format(url))
        img_file = urllib2.urlopen(url).read()
        img_array = np.asanyarray(bytearray(img_file), dtype=np.int8)
        img = cv2.imdecode(img_array, cv2.IMREAD_UNCHANGED)
    except Exception,e:
        if logging:
            logging.warning('[load_imgFromUrl] load image, error:{}\n'.format(e))
    return img

#hash编码
def avhash(im):
    if not isinstance(im, Image.Image):
        im = Image.open(im)
    im = im.resize((8, 8), Image.ANTIALIAS).convert('L')
    avg = reduce(lambda x, y: x + y, im.getdata()) / 64.
    return reduce(lambda x, (y, z): x | (z << y),
                  enumerate(map(lambda i: 0 if i < avg else 1, im.getdata())),
                  0)
#将本地图像，进行hash编码
def hashcode(filename):
    h = avhash(filename)
    h = bin(h).replace('0b', '')
    h = '{:0>64}'.format(h)
    code = tuple([int(i) for i in h])
    return code

#将cv2读取的图像，进行hash编码
def encode_img2hash(img):
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    im = Image.fromarray(img)
    h = avhash(im)
    h = bin(h).replace('0b', '')
    h = '{:0>64}'.format(h)
    #code = tuple([int(i) for i in h])
    return h

#根据autohome_list中的数据，下载图像并进行hash编码，跟新本地数据库
def update_code(database, autohome_list, logging=None):
    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        for auto in autohome_list:
            #print auto['sourcepic']
            img = load_imgFromUrl(auto['sourcepic'])
            #print 'img.shape = ', img.shape
            if img == []:
                continue
            try:
                code = encode_img2hash(img)
            except Exception,e:
                logging.info('[update_code] encode_img2hash url:{} error:{}'.format(auto['sourcepic'], e))
                continue

            updata_sql = "update autohome set code1 = '{}', update_time = '{}' where infoid = '{}'"
            try:
                cur_time = time.strftime(format_db, time.localtime())
                comm = updata_sql.format(code, cur_time, auto['infoid'])
                if logging:
                    logging.debug(str('[update_code] ')+comm)
                cursor.execute(comm)
                conn.commit()

                output_file = os.path.basename(auto['sourcepic'])
                output_file = os.path.join('./autohome_pic', output_file)
                #print output_file
                cv2.imwrite(output_file, img)
            except Exception,e:
                if logging:
                    logging.warning('[update_code] update autohome, error:{}\n'.format(e))
 
        cursor.close()
        conn.close()

                
#创建数据库，并建表                
def setup_db(database, logging=None):

    setup_sql = """CREATE TABLE autohome(
                infoid char(64) primary key not null, 
                brandid int not null, 
                brandname char(64) not null, 
                seriesid int not null, 
                seriesname char(64) not null, 
                url char(10240) not null, 
                code1 char(1024), 
                code2 char(1024), 
                code3 char(1024), 
                code4 char(1024),
                add_time DATETIME,
                update_time DATETIME
                );
           """

    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        try:
            if logging:
                logging.info(str('[setup_db] ')+setup_sql)
            cursor.execute(setup_sql)
            conn.commit()
        except Exception,e:
            if logging:
                logging.warning('[setup_db] create table autohome, error:{}\n'.format(e))
        
        cursor.close()
        conn.close()


#创建数据库，并建表                
def setup_logdb(database, logging=None):
    
    setup_sql = """CREATE TABLE visit(
                id char(64) primary key not null, 
                operator char(64) not null, 
                tag int not null, 
                source char(64) not null, 
                start_time DATETIME not null, 
                end_time DATETIME not null, 
                count int not null,
                add_time DATETIME
                );
           """

    conn = sqlite3.connect(database)
    if conn is not None:
        cursor = conn.cursor()
        try:
            if logging:
                logging.info(str('[setup_logdb] ')+setup_sql)
            cursor.execute(setup_sql)
            conn.commit()
        except Exception,e:
            if logging:
                logging.warning('[setup_logdb] create table visit, error:{}\n'.format(e))
        
        cursor.close()
        conn.close()
    
def get_fetch_time(database, logging=None):
    
    select_sql = """select * from visit where operator == 'insert' order by add_time desc"""
    conn = sqlite3.connect(database)
    result = None
    if conn is not None:
        cursor = conn.cursor()
        try:
            if logging:
                logging.info(str('[get_fetch_time] ')+select_sql)
            cursor.execute(select_sql)
            result = cursor.fetchall()
        except Exception,e:
            if logging:
                logging.warning('[get_fetch_time] select * from autohome, error:{}\n'.format(e))
        cursor.close()
        conn.close()  

    if result:
        if logging:
            logging.debug('[get_fetch_time] [result] '+str(result[0]))
        start_time = result[0][4]
        end_time = result[0][5]

        start_time = time.strftime(format_2sc, time.strptime(end_time, format_db))
        end_time = time.strftime(format_2sc, time.localtime())
    else:
        end_time = time.strftime(format_2sc, time.localtime())
        start_time = update_time(end_time, -100000)
    return start_time, end_time

#定义时间转换函数
def update_time(last_time, duration=86400):
    new_time = time.localtime(time.mktime(time.strptime(last_time, format_2sc)) + duration)
    new_time = time.strftime(format_2sc, new_time)
    return new_time

    

# In[204]:

#setup_db('./sqlit3/au.db')


# In[145]:

#autohome_list = lookup_null('./sqlit3/autohome_2sc.db')


# In[162]:

#update_code('./sqlit3/autohome_2sc.db', autohome_list)


# In[125]:

#load_and_setup('./sqlit3/autohome_2sc.db', '../在售车源20170627.csv')


# In[105]:

#fetch_new_and_insert(url_format, '2017-6-27__00_00_00', '2017-6-28__12_00_00', './sqlit3/autohome_2sc.db')


# In[108]:

#fetch_and_delete(url_format, '2017-6-27__00_00_00', '2017-6-28__12_00_00', './sqlit3/autohome_2sc.db')


# In[103]:

#fetch_new_and_insert(url_format, '2017-6-27__00_00_00', '2017-6-28__12_00_00', './sqlit3/autohome_2sc.db')


# In[166]:

#cur_time = time.localtime()


# In[167]:

#cur_time


# In[171]:

#print time.localtime(time.time())


# In[173]:

#time.mktime(cur_time)


# In[174]:

#time.time()


# In[175]:

#time.localtime()


# In[176]:

#begin = '2017-06-28 12:23:00'


# In[177]:

#time.strptime(begin, "%Y-%m-%d %H:%M:%S")


# In[179]:

#time.mktime(time.strptime(begin, "%Y-%m-%d %H:%M:%S"))+10


# In[182]:

#new_time = time.localtime(time.mktime(time.strptime(begin, "%Y-%m-%d %H:%M:%S"))+10)
#new_time


# In[183]:

#time.strftime("%Y-%m-%d %H:%M:%S", new_time)


# In[ ]:




# In[197]:




# In[198]:

#begin = '2017-06-28__12-23-00'
#new_time = update_time(begin, 3600*14)


# In[199]:

#new_time


# In[ ]:



