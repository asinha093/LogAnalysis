__author__ = 'rahul'
from pyspark_cassandra.context import *
import datetime
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import random
import pyspark_cassandra
from pyspark import SparkConf, SparkContext
def retrv_stat(rdd):
    ip_count = rdd.map(lambda r: (r["host"], 1)).reduceByKey(lambda a, b: a + b).collect()
    link_count = rdd.map(lambda r: (r["reqtlink"], 1)).reduceByKey(lambda a, b: a + b).collect()
    vm_count = rdd.map(lambda r: (r["virt_mach"], 1)).reduceByKey(lambda a, b: a + b).collect()
    rdd.unpersist()
    return ip_count, link_count, vm_count

conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
sc = CassandraSparkContext(conf=conf)
rdd = sc.cassandraTable("main", "random2")
pool = ConnectionPool('main',['localhost:9160'])
try:
    col_fam = ColumnFamily(pool,'random2')
except:
    print "Connection Error"
t1 = datetime.datetime.now()
total = 0
time_count = {}
error_ip1 = retrv_stat(rdd.filter(lambda row: row.response >= '200').filter(lambda row: row.response < '300').cache())
error_ip2 = retrv_stat(rdd.filter(lambda row: row.response >= '300').filter(lambda row: row.response < '400').cache())
error_ip3 = retrv_stat(rdd.filter(lambda row: row.response >= '200').filter(lambda row: row.response < '500').cache())
error_ip4 = retrv_stat(rdd.filter(lambda row: row.response >= '500').cache())
print datetime.datetime.now() - t1
print error_ip2[2]




