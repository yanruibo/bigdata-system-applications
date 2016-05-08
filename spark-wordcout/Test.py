#!/usr/bin/python
# -*- coding:utf-8 -*-

'''
Created on Oct 26, 2015

@author: yanruibo
'''
import re

def map_filter(s):
    #正则表达式　不区分大小写匹配不以字母开头的字符串
    p = re.compile('(?i)^[^a-zA-Z]+')
    m = p.match(s)
    end = 0
    if(m):
        #如果匹配上，返回匹配上的字符的尾部位置
        end = m.end()
    #将前面的不要的字符截掉
    s = s[end:]
    #正则表达式　不区分大小写匹配不以字母结尾的字符串
    p = re.compile('(?i)[^a-zA-Z]+$')
    m = p.match(s)
    start = None
    if(m):
        start = m.start()
        s = s[:start]
        print 'start',start
    
    if(len(s) == 0):
        return ('', 0)
    return (s, 1)
        
if __name__ == '__main__':
    '''
    fw = open("result1.txt","w")
    for line in open("content.txt"):
        s = line.split("\t")[0]
        (s,num) = map_filter(s)
        fw.write(s+"\n")
    fw.close()
    '''
    s = '''"/de/"'''
    (s,num) = map_filter(s)
    print s
    
