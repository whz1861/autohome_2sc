








import sys
from utils import *
import argparse
import logging, logging.handlers
import time
import yaml
import json

url_format = format('http://2sc.api.che168.com/car/GetCarBrandSeriesfirstPicByTime.ashx?_appid={}sc&flag={}&btime={}&&etime={}')

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database', default='')
    parser.add_argument('--data', default='./sqlite/online.csv')
    parser.add_argument('--cfg', default='')
    return parser.parse_args()


def timedRotatingFileHandler():
    fmt_str = '%(asctime)s[level-%(levelname)s][%(name)s]:%(message)s'

    logging.basicConfig()

    fileshandle = logging.handlers.TimedRotatingFileHandler('./log/main', when='H', interval=1, backupCount=0)
    fileshandle.suffix = "%Y%m%d_%H%M%S.log"
    fileshandle.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt_str)
    fileshandle.setFormatter(formatter)
    logging.getLogger('').addHandler(fileshandle)

    
def server(database, data):
    timedRotatingFileHandler()

    count = 0
    while True:
        begin = time.time()
        cur_date = time.strftime('%Y-%m-%d', time.localtime())
        #log_file = cur_date+str('_main.log')
        #log_file = os.path.join('./log', log_file)
        #print count, log_file
        #logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(filename)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', filename=log_file, filemode='a')
    
        #the first time: create database and tables
        if not os.path.exists(database):
            setup_db(database, logging)
            setup_logdb(database, logging)
            load_and_setup(database, data, logging) 


        #fetech new data from autohome
        #get start_time
        start = time.time()
        start_time,end_time = get_fetch_time(database, logging)
        end = time.time()
        print '----fetch time = ', start_time,end_time
        logging.info('[main] fetch time: {}.s'.format(end-start))       
 
        #fetch new data
        start = time.time()
        fetch_new_and_insert(url_format, start_time, end_time, database, 'new', logging)
        end = time.time()
        print '----fetch_new_and_insert time = ', (end-start), '.s'
        logging.info('[main] fetch new and insert time:{}.s'.format(end-start))

        #fetch delete data
        start = time.time()
        fetch_and_delete(url_format, start_time, end_time, database, logging)
        end = time.time()
        print '----fetch_and_delete time = ', (end-start), 's'
        logging.info('[main] fetch and delete time:{}.s'.format(end-start))
        
        #select data where code is null
        start = time.time()
        lookup_null(database, logging)
        end = time.time()
        print '----lookup_null time = ', (end-start), '.s'
        logging.info('[main] lookup null time:{}.s'.format(end-start))
        
        duration = (end - begin)
        if duration < 3600:
            print 'please wait for {}.s'.format(int(3600-duration))
            time.sleep(int(3600-duration))


if __name__ == '__main__':
    args = get_args()
    if args.database == '':
        print 'Usage: python main.py --database database.db --data data.csv --cfg cfg.cfg'
        sys.exit()
    
    while True:    
        try: 
            server(args.database, args.data)
            #test_log()
        except Exception,e:
            with open('./log/error.log', 'a') as f:
                cur_time = cur_date = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
                f.write('{}\t server error:{}'.format(cur_time, e))  

