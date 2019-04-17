#!/usr/bin/python3

import pymysql
import datetime
from Crypto.Cipher import AES
import binascii
from hashlib import sha1


def get_table_cols(cursor,dbname,tabname):

    sql = "select group_concat(concat('`',column_name,'`'))  from information_schema.COLUMNS \
    where TABLE_SCHEMA='"+dbname+"' and table_name='"+tabname+"'"
    try:
        cursor.execute(sql)
        result=cursor.fetchone()
        return result[0]
    except:

        return  0


def get_tables(cursor,dbname):
    sql="select a.TABLE_SCHEMA,a.TABLE_NAME from information_schema.TABLES a,information_schema.COLUMNS b \
    where a.ENGINE='InnoDB' \
    and a.table_schema ='"+dbname+"' \
    and a.TABLE_NAME=b.TABLE_NAME \
    and a.table_schema=b.table_schema \
    and b.column_name='id'"
    try:

        cursor.execute(sql)
        tables=cursor.fetchall()
        return tables
    except:
        return 0


def insert_checksums_table(db,dbname,tabname):
    cursor = db.cursor()
    cols=get_table_cols(cursor,dbname,tabname)

    sql = "REPLACE INTO `percona`.`checksums` \
            (db, tbl, chunk, chunk_index, lower_boundary, upper_boundary, this_cnt, this_crc) \
            SELECT '" + dbname + "', '" + tabname + "', '1', 'PRIMARY', '1', '1000', COUNT(*) AS cnt, \
            COALESCE(LOWER(CONV(BIT_XOR(CAST(CRC32(CONCAT_WS('#'," + cols + ")) AS UNSIGNED)), 10, 16)), 0) AS crc  \
            FROM `" + dbname + "`.`" + tabname + "` FORCE INDEX(`PRIMARY`) WHERE ((`id` >= '1')) AND ((`id` <= '1000'))"

    if cols:
        try:
            set_session_variables(cursor)
            cursor.execute("select  @@binlog_format")
            if cursor.fetchone()[0] == "STATEMENT":

                starttime = datetime.datetime.now()

                cursor.execute(sql)
                db.commit()
                endtime = datetime.datetime.now()

                chunk_time=round(endtime.timestamp() - starttime.timestamp(),6)
            else:
                chunk_time = 0
            return chunk_time
        except:
            db.rollback()

            print(dbname + '.' + tabname + "表无主键，请添加主键！")
            return 0


def create_checksum_table(cursor):

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
        return 0


def update_checksum_table(db,chunk_time,this_crc,this_cnt,dbname,tabname):
    cursor = db.cursor()
    sql="UPDATE `percona`.`checksums` \
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
        cursor.execute("SET @@binlog_format = 'STATEMENT'")
        cursor.execute('SET @@SQL_QUOTE_SHOW_CREATE = 1')
        return 1
    except :
        print("账号可能没有Super权限！")
        return 0

"""
def get_SH_host():
    slave_list = []
    db = pymysql.connect("192.168.1.154", "dbamgr", "De0ca71106a4e4d1")

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor.execute("show processlist")

    for res in cursor.fetchall():

        if re.match('Binlog Dump',res[4]):
            host = res[2].split(':',1)[0]
            slave_list.append(host)


    db.close()
    return  slave_list
"""
def source(host,port,username,password,dbname):
    # 打开数据库连接
    db = pymysql.connect(host, username, password,None,port)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    create_checksum_table(cursor)

    tables=get_tables(cursor,dbname)

    for tabschema,tabname in tables:
        chunk_time = insert_checksums_table(db,tabschema,tabname)
        if chunk_time:
            sql="select this_crc,this_cnt from percona.checksums where db='" +tabschema+"' \
            and tbl='"+tabname+"' and chunk=1"

            cursor.execute(sql)
            results=cursor.fetchone()

            crc=results[0]

            count=results[1]
        else:
            chunk_time=0
            crc='0'
            count=0

        update_checksum_table(db,chunk_time, crc, count, dbname, tabname)

    # 关闭数据库连接
    db.close()

def target(host,port,username,password):

    db = pymysql.connect(host, username, password, "percona",port)


    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    sql="""SELECT
        CONCAT(db, '.', tbl)
        AS
        `table`, chunk, chunk_index, lower_boundary, upper_boundary, COALESCE(this_cnt - master_cnt, 0)
        AS cnt_diff, COALESCE(this_crc <> master_crc
        OR ISNULL(master_crc) <> ISNULL(this_crc), 0) AS
        crc_diff, this_cnt, master_cnt, this_crc, master_crc
        FROM `checksums`
        WHERE(master_cnt <> this_cnt
        OR master_crc <> this_crc
        OR ISNULL(master_crc) <> ISNULL(this_crc))"""
    cursor.execute(sql)
    result=cursor.fetchall()
    print(result)

    db.close()

#解密
def decrypt(data, key):
    """aes解密
    :param key:
    :param data:
    """
    cipher = AES.new(key, AES.MODE_ECB)
    result = binascii.a2b_hex(data)  # 十六进制还原成二进制
    decrypted = cipher.decrypt(result)
    return decrypted.rstrip(b'\x10')  # 解密完成后将加密时添加的多余字符'\0'删除

"""
加密函数
def encrypt(text,key):
    cryptor = AES.new(key, AES.MODE_ECB)
    # 这里密钥key 长度必须为16（AES-128）、24（AES-192）、或32（AES-256）Bytes 长度.目前AES-128足够用
    length = 16
    count = len(text)
    if (count % length != 0):
        add = length - (count % length)
    else:
        add = 0
    text = text + ('\x10' * add)
    ciphertext = cryptor.encrypt(text)
        # 因为AES加密时候得到的字符串不一定是ascii字符集的，输出到终端或者保存时候可能存在问题
        # 所以这里统一把加密后的字符串转化为16进制字符串
    return binascii.b2a_hex(ciphertext)
"""


if __name__ == '__main__':

    db = pymysql.connect("rm-bp16270lw98n23fy0po.mysql.rds.aliyuncs.com", "qauser", "Qauser123", "dmsdb",3306)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    cursor.execute("SELECT id,host,port,username,password FROM `dmsdb`.`datasource` where name='percona1'")

    s = cursor.fetchone()

    source_id = s[0]
    source_host = s[1]
    source_port = s[2]
    source_username = s[3]
    key = source_host+source_username
    s1 = sha1()
    s1.update(key.encode())
    key = s1.digest()
    source_password = decrypt(s[4],key[:16])



    cursor.execute("SELECT sid,host,port,username,password FROM `dmsdb`.`datasource` where main_id='"+str(source_id)+"'")

    t = cursor.fetchone()

    target_db = t[0]
    target_host = t[1]
    target_port = t[2]
    target_username = t[3]
    key = target_host+target_username
    s1 = sha1()
    s1.update(key.encode())
    key = s1.digest()
    target_password = decrypt(t[4],key[:16])

    source(source_host,source_port,source_username,source_password,"percona")


    target(target_host,target_port,target_username,target_password)

"""                TS ERRORS    DIFFS     ROWS  CHUNKS SKIPPED   TIME TABLE
04-15T14:14:27      0      5   262144       6       0   1.637    testdb.a
ALTER TABLE `times` 
	CHANGE COLUMN `a` `id` int(11) NOT NULL AUTO_INCREMENT FIRST,
	DROP PRIMARY KEY,
	ADD PRIMARY KEY(`id`);
"""


