#!/usr/bin/python
# -*- coding:utf-8 -*-

'''
Created on Oct 26, 2015

@author: yanruibo
'''
import re
'''
去掉map中的函数时每个word中首尾的标点符号和字母，并将不为空的置为１，
为空的置为('', 0)
因为spark接口必须要求返回一个数据
'''
def map_filter(s):
    #定义正则表达式匹配以非字母开头的字符串
    p = re.compile('(?i)^[^a-zA-Z]+')
    m = p.match(s)
    end = 0
    if(m):
        #如果匹配上，获得匹配位置的最后一个字符位置
        end = m.end()
    s = s[end:]
    #字符串反转
    s = s[::-1]
    #去掉末尾的特殊字符
    m = p.match(s)
    end = 0
    if(m):
        end = m.end()
    s = s[end:]
    #最后再反转回来
    s = s[::-1]
    if(len(s) == 0):
        return ('', 0)
    return (s, 1)
