__author__ = 'rahul'

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *
import multiprocessing
import datetime
global col_fam,session
pool = ConnectionPool('main',['localhost:9160'])
col_fam = ColumnFamily(pool,'random2')
cluster = Cluster()
session = cluster.connect('main')


def insert_cassandra(uid, host, links, user_agent, byte_transfer, response_time):
    qwe = session.execute("""INSERT INTO ipdata (uid, host, links, user_agent, byte_transfer, response_time) VALUES (%s, %s, %s, %s, %s, %s)""",(uid, host, links, user_agent, byte_transfer, response_time))
    return 1

def retrv_stats(keys, host, num):
    links, vir_mem ,reqtype, userdata = [], [], [], []
    bytes, avg_time, count, unique_visits = 0, 0, 0, 0
    temp1 = []
    for x in keys:
        temp1.append(str(x))
    data = col_fam.multiget(temp1)
    for i in data.values():
        links.append(i["reqtlink"])
        userdata.append(i['user_data'])
        if i['byte_transfer'] != '-':
            bytes += int(i['byte_transfer'])
        if i['response_time'] != '-':
            avg_time += int(i['response_time'])
            count += 1
    avg_time = avg_time/count
    return insert_cassandra(num, host, links, userdata, bytes, avg_time)

def unique_count(set):
    temp = []
    temp2 = []
    for x in range(0,len(set)):
        if set[x] not in temp:
            temp.append(set[x].encode("utf-8"))
            temp2.append(set.count(set[x]))
        else:
            continue
    return temp, temp2


def initilize_conn():
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable("main", "random2").cache()
    time_sort = rdd.select("host","key").groupByKey().collect()
    total = 0
    count = 0
    num = 100000
    t7 = datetime.datetime.now()
    print len(time_sort)
    for x in time_sort:
        t = datetime.datetime.now()
        #if len(x[1]) > 5:
        retrv_stats(x[1], x[0],num)
        #else:
        #    insert_cassandra(num,x[0],['-'],[0],['-'],[0],0,0)
        total += len(x[1])
        num += 1
        count += 1
        print total, count, len(time_sort)
        print datetime.datetime.now()- t, datetime.datetime.now() - t7

    print 'done'
if __name__ == '__main__':
    initilize_conn()



