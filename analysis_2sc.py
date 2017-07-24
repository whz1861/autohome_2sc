
# coding: utf-8

# In[55]:

import os
import sys
import numpy as np
import urllib2
from urllib2 import HTTPError, URLError
import cv2
import matplotlib.pyplot as plt
from PIL import Image
import time
import argparse
#import car_recog
#import kd_tree2


# In[2]:

# ####加载用于车系识别训练的类别标签2836类

# In[3]:

def load_bst(filename):
    bst_list = []
    with open(filename, 'r') as f:
        while 1:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            bst_list.append(line)
    return bst_list


# ####加载二手车的图像url列表信息

# In[4]:

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
            auto['brand_id'] = lines[0]
            auto['brand_name'] = lines[1]
            auto['series_id'] = lines[2]
            auto['series_name'] = lines[3]
            auto['url'] = lines[4]
            autohome_list.append(auto)
    return autohome_list


# ####下载url图像

# In[59]:

def load_imgFromUrl(url):
    img = []
    try:
        img_file = urllib2.urlopen(url).read()
        img_array = np.asanyarray(bytearray(img_file), dtype=np.int8)
        img = cv2.imdecode(img_array, cv2.IMREAD_UNCHANGED)
    except Exception,e:
        print e
    return img


# ####hash编码

# In[82]:

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


# ####生成hash编码

# In[43]:

def gen_hashcode(autohome_list, bst_list):
    bs_code_list = {}
    for index,auto in enumerate(autohome_list):
        #提取车型，车系信息，并判断该车型是否存在已知的训练车型中
        bs = auto['brand_id']+str('.')+auto['series_id']
        key = ''
        try:
            loc = bst_list.index(bs)
            key = bs
        except ValueError,e:
            loc = -1
            key = '0000.0000'
        if auto['url'].find('http://') == -1:
            continue
        #生成hash编码
        start_load = time.time()
        img = load_imgFromUrl(auto['url'])
        if len(img) == 0:
            continue
        end_load = time.time()
        code = encode_img2hash(img)
        end_encode = time.time()
        print "%d,load/encode[%.2f/%.2f] - " % (index, (end_load-start_load), (end_encode-end_load)),
        if key not in bs_code_list:
            bs_code_list[key] = {}
        bs_code_list[key][auto['url']] = code
    
    return bs_code_list


# ####下载图像，并生成hash表，根据车型车系类别进行保存

# In[90]:

def save(autohome_list, bst_list, path, begin, savepic=False):
    for index,auto in enumerate(autohome_list[begin:]):
        #提取车型，车系信息，并判断该车型是否存在已知的训练车型中
        bs = auto['brand_id']+str('.')+auto['series_id']
        key = ''
        try:
            loc = bst_list.index(bs)
            key = bs
        except ValueError,e:
            loc = -1
            key = '0000.0000'
        if auto['url'].find('http://') == -1:
            continue
        #生成hash编码
        start_load = time.time()
        img = load_imgFromUrl(auto['url'])
        if len(img) == 0:
            continue
        end_load = time.time()
        code = encode_img2hash(img)
        end_encode = time.time()
        content = "%d,load/encode[%.2f/%.2f] - " % (index+begin, (end_load-start_load), (end_encode-end_load)),
        #with open('log.txt', 'wa') as f:
        #    f.write(content)
        print 'content = ', content,        


        #生成输出图片名
        output_path = os.path.join(path, key)
        output_name = os.path.join(output_path, os.path.basename(auto['url']))
        print output_name
        with open('log.txt', 'a') as f:
            f.write(content[0]+ output_name + str('\n'))        

        if savepic:
            if not os.path.exists(output_path):
                os.makedirs(output_path)
            cv2.imwrite(output_name, img)
        
        #生成输出code文件名
        output_file = os.path.join(path, key)
        output_file = output_file + str('.code')
        
        with open(output_file, 'a') as f:
            content = auto['url'] + str(' ') + str(code) + str('\n')
            f.write(content)
        
    return


def get_index():
    index_max = 0
    with open('log.txt', 'r') as f:
        while 1:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            index = int(line.split(',')[0])
            if index > index_max:
                index_max = index
    return index_max


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default='autohome_2sc.csv')
    parser.add_argument('--bst', default='train_car.list')
    parser.add_argument('--result', default='autohome_2sc')
    parser.add_argument('--savepic', default='False')
    return parser.parse_args()



if __name__ == '__main__':

    if len(sys.argv) == 1:
        print "Usage: python analysis_2sc.py --data[csv] autohome_2sc.csv --bst train_car.list"
        sys.exit()
    args = get_args()

    begin = get_index()
    autohome_list = load_autohome(args.data)
    bst_list = load_bst(args.bst)


    save(autohome_list, bst_list, args.result, begin+1, args.savepic)



