#!/usr/bin/python3

import pymysql
import datetime
import time


def set_session_variables(cursor):

    #SET @@binlog_format = 'STATEMENT'; 阿里云没有开发super权限账号,因此不支持
    cursor.execute('SET SESSION innodb_lock_wait_timeout = 1')
    cursor.execute('SET SESSION wait_timeout = 10000')
    cursor.execute("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION'")

    cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
    cursor.execute("SET @@binlog_format = 'STATEMENT'")
    cursor.execute('SET @@SQL_QUOTE_SHOW_CREATE = 1')


def source(host,port,username,password,dbname):
    # 打开数据库连接
    db = pymysql.connect(host, username, password,None,port)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 创建checksum表
    sql="""CREATE TABLE if not exists percona.`checksums` (\
    `db` char(64) NOT NULL,\
    `tbl` char(64) NOT NULL,\
    `chunk` int(11) NOT NULL,\
    `chunk_time` float DEFAULT NULL,\
    `chunk_index` varchar(200) DEFAULT NULL,\
    `lower_boundary` text,\
    `upper_boundary` text,\
    `this_crc` char(40) NOT NULL,\
    `this_cnt` int(11) NOT NULL,\
    `master_crc` char(40) DEFAULT NULL,\
    `master_cnt` int(11) DEFAULT NULL,\
    `ts` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\
    PRIMARY KEY (`db`,`tbl`,`chunk`),\
    KEY `ts_db_tbl` (`ts`,`db`,`tbl`)\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
    try:
        cursor.execute(sql)
        cursor.execute("truncate percona.checksums")
    except:
        print("checksums表初始化失败！")

    #获取目标库下需要比对的表清单
    sql="select a.TABLE_SCHEMA,a.TABLE_NAME from information_schema.TABLES a,information_schema.COLUMNS b \
    where a.ENGINE='InnoDB' \
    and a.table_schema ='"+dbname+"' \
    and a.TABLE_NAME=b.TABLE_NAME \
    and a.table_schema=b.table_schema \
    and b.column_name='id'"
    try:

        cursor.execute(sql)
        chktablelist=cursor.fetchall()
    except:
        chktablelist=[]

    #循环遍历表清单做数据分批次做checksum
    for dbname,tname in chktablelist:
        #获取表的所有字段
        colsql = "select group_concat(concat('`',column_name,'`'))  from information_schema.COLUMNS \
        where TABLE_SCHEMA='" + dbname + "' and table_name='" + tname + "'"
        try:
            cursor.execute(colsql)
            result = cursor.fetchone()
            cols= result[0]
        except:
            print('获取列失败！')
            break
        #获取表的记录总数
        cntsql = "select max(id) from " + dbname + "." + tname
        try:
            cursor.execute(cntsql)
            result = cursor.fetchone()
            maxid = result[0]
        except:
            print('获取当前最大id！')
            break
        #对本批次数据做checksum  插入到checksums表中
        chunk = 1

        for key in range(1,maxid,10000):
            endkey = key + 9999
            if endkey > maxid:
                checksum_dml = "REPLACE INTO `percona`.`checksums` \
                                    (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) \
                                    SELECT '" + dbname + "', '" + tname + "', " + str(chunk) + ", 'PRIMARY', " + str(maxid) + ", NULL, COUNT(*) AS cnt, \
                                    0 AS crc  \
                                    FROM `" + dbname + "`.`" + tname + "` FORCE INDEX(`PRIMARY`) WHERE `id` >"+ str(maxid)
            elif endkey< maxid and key+20000> maxid:
                checksum_dml = "REPLACE INTO `percona`.`checksums` \
                                    (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) \
                                    SELECT '" + dbname + "', '" + tname + "', " + str(chunk) + ", 'PRIMARY', " + str(
                    key) + ", " + str(maxid) + ", COUNT(*) AS cnt, \
                                    COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#'," + cols + ")) AS UNSIGNED)), 10, 16)), 0) AS crc  \
                                    FROM `" + dbname + "`.`" + tname + "` FORCE INDEX(`PRIMARY`) WHERE `id` between " + str(
                    key) + " and " + str(maxid)
            else:
                checksum_dml = "REPLACE INTO `percona`.`checksums` \
                    (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) \
                    SELECT '" + dbname + "', '" + tname + "', "+ str(chunk) +", 'PRIMARY', "+ str(key) +", "+str(endkey)+", COUNT(*) AS cnt, \
                    COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#'," + cols + ")) AS UNSIGNED)), 10, 16)), 0) AS crc  \
                    FROM `" + dbname + "`.`" + tname + "` FORCE INDEX(`PRIMARY`) WHERE `id` between " + str(key) + " and " + str(endkey)

            #计算本批次任务执行时长
            chunk_time = 0
            try:
                db.commit()
                set_session_variables(cursor)
            except:
                print("session variables 设置失败！")
                break
            else:
                try:
                    cursor.execute("select  @@binlog_format")
                    if cursor.fetchone()[0] == "STATEMENT":
                        starttime = datetime.datetime.now()

                        cursor.execute(checksum_dml)
                        db.commit()
                        endtime = datetime.datetime.now()
                    else:
                        print("binlog格式为更改成STATEMENT")
                        break
                except:
                    db.rollback()
                    print("checksum表插入失败！")
                else:
                    chunk_time = round(endtime.timestamp() - starttime.timestamp(), 6)
            #获取本批次crc值和行数值

            sql="select this_crc,this_cnt from percona.checksums where db='" +dbname+"' \
            and tbl='"+tname+"' and chunk="+ str(chunk)
            cursor.execute(sql)
            results=cursor.fetchone()

            this_crc=results[0]

            this_cnt=results[1]


            #更新checksums表中master_crc和master_cnt
            sql = "UPDATE `percona`.`checksums` \
            SET chunk_time = '" + str(chunk_time) + "', master_crc = '" + this_crc + "', master_cnt = " + str(this_cnt) + " \
            WHERE db = '" + dbname + "' AND tbl = '" + tname + "' AND chunk = "+ str(chunk)

            try:
                cursor.execute(sql)
                db.commit()
                chunk += 1
            except:

                db.rollback()

    # 关闭数据库连接
    db.close()

def target(host,port,username,password,dbname):

    db = pymysql.connect(host=host, user=username, password=password, db=dbname, port=port)
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 查看主从同步状态是否正常有没有延迟
    cnt = 0
    for i in range(100):
        cmd = "show slave status"
        cursor.execute(cmd)
        result=cursor.fetchone()
        Slave_IO_Running = result[10]
        Slave_SQL_Running = result[11]
        if Slave_IO_Running == 'NO' or Slave_SQL_Running == 'NO':
            return "主从复制异常！" , None

        Seconds_Behind_Master = result[32]
        if Seconds_Behind_Master == 0:
            cnt+=1
            time.sleep(2)
        if cnt > 5:
            break


    #没有延迟的话从从库获取比对结果
    descsql="""SELECT
        CONCAT(db, '.', tbl) AS `table`, chunk, chunk_index, lower_boundary, upper_boundary,cast( COALESCE(this_cnt - master_cnt, 0) as char)
        AS cnt_diff, COALESCE(this_crc <> master_crc
        OR ISNULL(master_crc) <> ISNULL(this_crc), 0) AS
        crc_diff, cast(this_cnt as char), cast(master_cnt as char), this_crc, master_crc
        FROM `checksums`
        WHERE(master_cnt <> this_cnt
        OR master_crc <> this_crc
        OR ISNULL(master_crc) <> ISNULL(this_crc) )"""

    cursor.execute(descsql)
    diffs=cursor.fetchall()

    sql="select date_format(max(ts),'%Y-%m-%d %H:%I:%S') as TS, "+ str(len(diffs)) +" as DIFFS,cast(sum(master_cnt) AS   CHAR) as ROWS, max(chunk) as CHUNKS,round(sum(chunk_time),3) as TIME,concat(db,'.',tbl) as  'TABLE'  from checksums"
    cursor.execute(sql)
    result = cursor.fetchall()

    db.close()

    return result,diffs


def do(argv):

    source(argv['m_host'], argv['m_port'], argv['m_user'], argv['m_password'], argv['m_db'])

    result, diffs = target(argv['s_host'], argv['s_port'], argv['s_user'], argv['s_password'], "percona")
    print(result, diffs)
    return result, diffs





