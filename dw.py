#!/usr/bin/env python3
#coding:utf-8
#Time:2019/5/20 9:15
#write_by :yebinjie
#script_name:dw.py

import pymysql

db = pymysql.connect(host='192.168.1.141',port=3307,user='dev',password='dev',db='testdb')

corsor = db.cursor()

while True:
    sql= "insert into a (a,b) values (floor(rand()*100),'aaa')"

    corsor.execute(sql)
    db.commit()
