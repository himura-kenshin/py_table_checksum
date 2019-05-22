#!/usr/bin/env python3
#coding:utf-8
#Time:2019/5/20 9:15
#write_by :yebinjie
#script_name:dw.py

import pymysql
import random

def randstr(n):
    word="abcdefghijklmnopqrstuvwxyz"
    w=""
    for i in range(n):
        index=random.randint(0,25)
        w = w+word[index]
    return w


db = pymysql.connect(host='192.168.1.141',port=3307,user='dev',password='dev',db='testdb')

corsor = db.cursor()

while True:
    #sql= "insert into a (a,b) values (floor(rand()*100),'aaa')"
    sql = "update a set b='"+randstr(8)+"' where a=floor(rand()*100)"
    corsor.execute(sql)
    db.commit()
