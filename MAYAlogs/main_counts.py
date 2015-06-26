from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *

import multiprocessing
import datetime
global col_fam,session
pool = ConnectionPool('main',['localhost:9160'])
cluster = Cluster()
session = cluster.connect('main')



def insert_cassandra(uid,vm_list, vm_count, req_list, req_count,os_list, os_count, device_list, device_count):
    query = "UPDATE  main_counts SET  vm_list=%s, vm_count=%s, req_list=%s, req_count=%s, os_list=%s, os_count=%s, device_list=%s, device_count=%s WHERE uid=%s"
    session.execute(query, parameters=[vm_list, vm_count, req_list, req_count,os_list,os_count, device_list, device_count, uid])
    return 1


def initilize_conn():
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable("main", "parsed_data").cache()
    query = "SELECT uid FROM main_counts"  # users contains 100 rows
    t = session.execute(query)
    for val in t :
        uid = val[0]

    vm_rdd = rdd.select("virt_mach","uid").groupByKey().collect()
    vm_list, vm_count = [], []
    for x in vm_rdd:
        vm_list.append(x[0])
        vm_count.append(len(x[1]))
    reqtype_list, reqtype_count = [], []
    reqtype_rdd = rdd.select("req_type","uid").map(lambda r: (r["req_type"], 1)).reduceByKey(lambda a, b: a + b).collect()
    for x in reqtype_rdd:
        reqtype_list.append(x[0])
        reqtype_count.append(x[1])
    os_list, os_count = [], []
    os_rdd = rdd.select("os","uid").groupByKey().collect()
    for x in os_rdd:
        os_list.append(x[0])
        os_count.append(len(x[1]))
    print os_list
    print os_count
    device_list, device_count = [], []
    device_rdd = rdd.select("phone_type","uid").groupByKey().collect()
    for x in device_rdd:
        device_list.append(x[0])
        device_count.append(len(x[1]))
    print device_list
    print device_count
    insert_cassandra(uid, vm_list, vm_count, reqtype_list, reqtype_count, os_list, os_count, device_list, device_count)


if __name__ == '__main__':
    initilize_conn()