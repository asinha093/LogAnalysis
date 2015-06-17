from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

import pyspark_cassandra
from pyspark import SparkConf, SparkContext
from pyspark_cassandra.context import *
from cassandra.cluster import Cluster

from datetime import datetime
global session, column_fam, key_space, t_init
t_init = datetime.now()
# connecting to the cassandra database to create a table
key_space = 'NASA_KS'
column_fam = 'NASA_CF'
session = Cluster(contact_points=['127.0.0.1'], port=9042).connect(keyspace=key_space)

def spark_config():
    
    # configuring spark with cassandra keyspace and columnfamily
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    RDD = sc.cassandraTable(key_space, column_fam)
    return start_connection(RDD)

def start_connection(rdd):

    # collecting rows in rdd grouped by key
    host_rdd = rdd.select("host","key").groupByKey().collect()
    pool = ConnectionPool(key_space, ['localhost:9160'], timeout=60)
    col_fam = ColumnFamily(pool, column_fam)
    uuid = 100000
    row_count = 1
    for hostkey in host_rdd:
        if hostkey[0][hostkey[0].rfind(".")+1 : ].isalpha():
            continue
        print "Row: %s" % row_count
        # function calls
        retrieve_stats(host_rdd, hostkey[1], hostkey[0], uuid, col_fam)
        uuid = uuid + 1
        row_count = row_count + 1
    return 1

def retrieve_stats(rdd, keys, host, uid, cass_conn):

    request_link, response_code, keytemp = [], [], []
    bytes = 0
    for key in keys:
        keytemp.append(str(key))
    # creating an ordered dictionary containing log data retrieved from column_family
    log = cass_conn.multiget(keytemp)
    for item in log.values():
        # appending lists with their respective values
        request_link.append(item['request-link']), response_code.append(item['response-code'])
        if item['byte-transfer'] != '-':
            bytes += int(item['byte-transfer'])
    return insert_stats(uid, host, request_link, response_code, bytes)

def insert_stats(num, host, reqlink, resp, bytes):

    t_ins = datetime.now()
    session.execute("""INSERT INTO NASA_IPDATA(uid, ip, request_link, response_code, byte_transfer) VALUES (%s, %s, %s, %s, %s)""",(num, host, reqlink, resp, str(bytes)))
    print "Time for inserting: %s  Time Elapsed: %s" %( (datetime.now() - t_ins), (datetime.now() - t_init) )
    return 1

if __name__ == "__main__":

    #session.execute("""CREATE TABLE NASA_IPDATA( uid bigint, ip varchar, request_link list<varchar>, response_code list<varchar>, byte_transfer varchar, PRIMARY KEY (uid) )""")
    #session.execute("""CREATE TABLE NASA_ERROR ( uid bigint, ip_400 list<varchar>, request_link_400 list<varchar>, ip_500 list<varchar>, request_link_500 list<varchar>, PRIMARY KEY (uid) )""")
    #function call
    spark_config()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)