import os
import sys
import json
import codecs
import subprocess
import time
import traceback

import pymysql


# 不带缓存输出数据
def printf(text):
    sys.stdout.write(text)
    sys.stdout.write("\n")
    sys.stdout.flush()

# 同步运行任务，如果命令异常则退出
def runCmd(cmdText):
    printf(cmdText)
    child_process = subprocess.Popen(cmdText, shell=True)
    (stdout, stderr) = child_process.communicate()
    sys.stdout.flush()
    printf('return code: %s, cmd: %s' %(child_process.returncode, cmdText))
    if (child_process.returncode):
        sys.exit(child_process.returncode)

# 创建目标数据库连接
def getConn():
    # 连接数据库
    conn = pymysql.connect(host='172.16.15.226',user='root',password='Fb@54321',db='example_db',port=7030)
    return conn

def executeSql(sql):
    # 获取数据库连接
    conn = getConn()
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 使用execute方法执行SQL语句
    cursor.execute(sql)
    # 使用 fetchone() 方法获取一条数据
    data = cursor.fetchall()
    # 关闭数据库连接
    cursor.close()
    conn.close()
    return data

def querySql(sql):
    # 获取数据库连接
    conn = getConn()
    # 使用cursor()方法获取操作游标
    cursor = conn.cursor()
    # 使用execute方法执行SQL语句
    cursor.execute(sql)
    # 使用 fetchone() 方法获取一条数据
    data = cursor.fetchall()
    # 关闭数据库连接
    cursor.close()
    conn.close()
    return data

if __name__ == '__main__':

    # 数据库名
    tabschema = 'example_db'
    # 表名
    tabname = 'table1'

    collist_sql = """select group_concat(column_name) as column_list
                       from (select column_name,
                                    ordinal_position
                               from information_schema.columns
                              where table_schema = '${tabschema}'
                                and table_name = '${tabname}'
                      order by ordinal_position) t;"""

    collist_sql = collist_sql.replace('${tabschema}',tabschema).replace('${tabname}',tabname)
    printf("查询字段的SQL：%s" % collist_sql)
    collist_str = querySql(collist_sql)[0][0]
    printf("查询到的字段列表：%s" % collist_str)

    label_name = 'load_' + tabname + '_' + time.strftime("%Y%m%d_%H%M%S", time.localtime())
    printf("查询到的字段列表：%s" % label_name)

    load_sql= """ LOAD LABEL ${tabschema}.${label_name}
    (
    DATA INFILE("hdfs://172.16.15.227:8020/user/hive/warehouse/doris.db/${tabname}/*")
    INTO TABLE ${tabname}
    COLUMNS TERMINATED BY ","
    (${column_list})
    )
    WITH BROKER broker_name 
    (
        "dfs.nameservices" = "hadoopcluster",
        "dfs.ha.namenodes.hadoopcluster" = "nn1,nn2",
        "dfs.namenode.rpc-address.hadoopcluster.nn1" = "172.16.15.226:8020",
        "dfs.namenode.rpc-address.hadoopcluster.nn2" = "172.16.15.227:8020",
        "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
    ) 
    PROPERTIES 
    (
        "timeout" = "3600", 
        "max_filter_ratio" = "0.1",
        "load_parallelism" = "8",
        "timezone"="Asia/Shanghai"
    );"""

    load_sql = load_sql.replace('${label_name}', label_name)\
        .replace('${tabschema}', tabschema)\
        .replace('${tabname}', tabname)\
        .replace('${column_list}', collist_str)

    printf("加载数据的SQL：%s" % load_sql)
    executeSql(load_sql)

    try:
        while True:
            status_sql = "SHOW LOAD FROM ${tabschema} WHERE LABEL ='${label_name}' ORDER BY LoadStartTime DESC;"
            status_sql = status_sql.replace('${label_name}', label_name).replace('${tabschema}',tabschema)
            printf("加载数据状态查询的SQL：%s" % status_sql)

            status_row = querySql(status_sql)[0]
            printf("状态查询到的数据：%s" % str(status_row))
            printf("状态查询到的状态：%s" % status_row[2])

            if status_row[2] == 'FINISHED':
                printf("%s load run sucess!" % label_name)
                break
            elif status_row[2] == 'LOADIND' or status_row[2] == 'PENDING':
                printf("%s load running! sleep 5s" % label_name)
                time.sleep(5)
            else:
                printf("%s load run failed!" % label_name)
                printf("ErrorMsg: %s " % status_row[7])
                printf("异常数据查看URL:%s" % status_row[13])
                break
    except Exception as e:
        traceback.print_exc() #输出程序错误位置
        os.close(1) #异常退出，终止程序

