from pyspark_cassandra.context import *
import datetime
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import random
import pyspark_cassandra
from pyspark import SparkConf, SparkContext
def retrv_stats(cass_conn,  keys):
    ip_add = []
    links = []
    vir_mem = []
    response_code = []
    for i in keys:
        i = str(i)
        x = cass_conn.get(i)
        if x['host']:
            ip_add.append(x['host'])
        if x['reqtlink']:
            links.append(x['reqtlink'])
        if x['virt_mach']:
            vir_mem.append(x['virt_mach'])
        if x['response']:
            response_code.append(x['response'])
    ipcount = unique_count(ip_add)
    link_count = unique_count(links)
    vm_count = unique_count(vir_mem)
    resp_count =  unique_count(response_code)
    return ipcount, link_count, vm_count, resp_count


def unique_count(set):
    temp = {}
    for i in range(0,len(set)):
        if set[i] in temp.keys():
            temp[set[i]] += 1
        else:
            temp[set[i]] = 1
    return temp


def initilize_conn():
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable("main", "random2")
    time_sort = rdd.select("timestamp","key").groupByKey().take(1000000)
    pool = ConnectionPool('main',['localhost:9160'])
    try:
        col_fam = ColumnFamily(pool,'random2')
    except:
        print "Connection Error"
    time_count = {}
    for x in time_sort:
        total += len(x[1])
        print 'retv status call '
        print datetime.datetime.now()
        total += len(x[1])
        time_count[x[0]] = retrv_stats(rdd, col_fam, x[0], x[1])
    print time_count[random.choice(time_count.keys())][0]

if __name__ == '__main__':
    initilize_conn()




