#!/usr/bin/python3

import pymysql
import datetime


def get_table_cols(cursor,dbname,tabname):

    sql = "select group_concat(concat('`',column_name,'`'))  from information_schema.COLUMNS \
    where TABLE_SCHEMA='"+dbname+"' and table_name='"+tabname+"'"
    try:
        cursor.execute(sql)
        result=cursor.fetchone()
        return result[0]
    except:

        return  0



def get_tables(cursor):
    sql="select a.TABLE_SCHEMA,a.TABLE_NAME from information_schema.TABLES a,information_schema.COLUMNS b \
    where a.ENGINE='InnoDB' \
    and a.table_schema not in ('information_schema','performance_schema','percona','sys','undolog','mysql') \
    and a.TABLE_NAME=b.TABLE_NAME \
    and a.table_schema=b.table_schema \
    and b.column_name='id'"
    try:
        cursor.execute(sql)
        tables=cursor.fetchall()
        return tables
    except:
        return 0


def insert_checksums_table(db,cursor,dbname,tabname):
    cols=get_table_cols(cursor,dbname,tabname)
    sql = "REPLACE INTO `wstest`.`checksums` \
            (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) \
            SELECT '" + dbname + "', '" + tabname + "', '1', 'PRIMARY', '1', '1000', COUNT(*) AS cnt, \
            COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#'," + cols + ")) AS UNSIGNED)), 10, 16)), 0) AS crc  \
            FROM `" + dbname + "`.`" + tabname + "` FORCE INDEX(`PRIMARY`) WHERE ((`id` >= '1')) AND ((`id` <= '1000'))"
    if cols:
        try:
            set_session_variables(cursor)
            starttime = datetime.datetime.now()

            cursor.execute(sql)
            db.commit()
            endtime = datetime.datetime.now()

            chunk_time=round(endtime.timestamp() - starttime.timestamp(),6)
            return chunk_time
        except:
            db.rollback()

            print(dbname + '.' + tabname + "表无主键，请添加主键！")
            return 0


def create_checksum_table(cursor):

    sql="""CREATE TABLE if not exists wstest.`checksums` (\
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
        cursor.execute("truncate wstest.checksums")
    except:
        return 0


def update_checksum_table(db,cursor,chunk_time,this_crc,this_cnt,dbname,tabname):
    sql="UPDATE `wstest`.`checksums` \
    SET chunk_time = '"+str(chunk_time)+"', master_crc = '"+this_crc+"', master_cnt = "+str(this_cnt)+" \
    WHERE db = '"+dbname+"' AND tbl = '"+tabname+"' AND chunk = 1"

    try:
        set_session_variables(cursor)
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()

def set_session_variables(cursor):

    #SET @@binlog_format = 'STATEMENT'; 阿里云没有开发super权限账号,因此不支持
    try:
        cursor.execute('SET SESSION innodb_lock_wait_timeout = 1')
        cursor.execute('SET SESSION wait_timeout = 10000')
        cursor.execute("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO,NO_ENGINE_SUBSTITUTION'")
        cursor.execute('SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        cursor.execute('SET @@SQL_QUOTE_SHOW_CREATE = 1')
        return 1
    except:
        return 0


def task_master():
    # 打开数据库连接
    db = pymysql.connect("rm-bp188fb70hknd9gi2o.mysql.rds.aliyuncs.com", "devuser", "Devuser123", "basedatadb")

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    create_checksum_table(cursor)

    tables=get_tables(cursor)

    for dbname,tabname in tables:
        chunk_time = insert_checksums_table(db,cursor,dbname,tabname)
        if chunk_time:
            sql="select this_crc,this_cnt from wstest.checksums where db='" +dbname+"' \
            and tbl='"+tabname+"' and chunk=1"

            cursor.execute(sql)
            results=cursor.fetchone()

            crc=results[0]

            count=results[1]
        else:
            chunk_time=0
            crc='0'
            count=0

        update_checksum_table(db,cursor,chunk_time, crc, count, dbname, tabname)

    # 关闭数据库连接
    db.close()

def check_slave():
    db = pymysql.connect("rr-bp1y172q0p1808b7klo.mysql.rds.aliyuncs.com", "devuser", "Devuser123", "wstest")

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    sql="""SELECT
    CONCAT(db, '.', tbl)
    AS
    `table`, chunk, chunk_index, lower_boundary, upper_boundary, COALESCE(this_cnt - master_cnt, 0)
    AS
    cnt_diff, COALESCE(this_crc <> master_crc
    OR
    ISNULL(master_crc) <> ISNULL(this_crc), 0) AS
    crc_diff, this_cnt, master_cnt, this_crc, master_crc
    FROM `wstest`.`checksums`
    WHERE(master_cnt <> this_cnt
    OR master_crc <> this_crc
    OR ISNULL(master_crc) <> ISNULL(this_crc))"""
    cursor.execute(sql)
    result=cursor.fetchall()
    print(result)

if __name__ == '__main__':
    task_master()
    check_slave()


