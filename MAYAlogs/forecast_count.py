from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *
from datetime import datetime
import time
from cassandra.query import BatchStatement
import uuid
global session, column_fam, key_space, t_init
t_init = datetime.now()
# connecting to the cassandra database to create a table
key_space = 'ASIA_KS'
column_fam = 'ASIA_CF'
session = Cluster(contact_points=['127.0.0.1'], port=9042).connect(keyspace=key_space)

def spark_config():
    
    # configuring spark with cassandra keyspace and columnfamily
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    RDD = sc.cassandraTable(key_space, column_fam)
    return start_connection(RDD)

def start_connection(rdd):
    
    time_rdd = rdd.select("timestamp","key").groupByKey().collect()
    pool = ConnectionPool(key_space, ['localhost:9160'], timeout=60)
    col_fam = ColumnFamily(pool, column_fam)
    batch = BatchStatement()
    row_count = 1
    for timekey in time_rdd:
        # function calls
        stats = retrieve_stats(timekey[1], timekey[0], col_fam)
        batch_prepare(batch, stats)
        if row_count % 300 == 0:
            # inserting in batches of 300
            insert_stats(batch)
            # creating a fresh batch
            batch = BatchStatement()
        row_count = row_count + 1
    insert_stats(batch)
    return 1

def retrieve_stats(keys, TIME, cass_conn):

    t1 = datetime.now()
    keytemp = []
    visits, requests, bytes, getcount, postcount, avg_time, count = 0,0,0,0,0,0,0
    for key in keys:
        keytemp.append(str(key))
    # creating an ordered dictionary containing log data retrieved from column_family
    log = cass_conn.multiget(keytemp)
    for item in log.values():
        # calculating total visits, requests, GET requests, POST requests, bytes and response-time for every timestamp
        if item['host'] != '-':
            visits += 1
        if item['request-type'] != '-':
            requests += 1
        if item['request-type'] == "GET":
            getcount += 1
        if item['request-type'] == "POST":
            postcount += 1
        if item['byte-transfer'] != '-':
            bytes += int(item['byte-transfer'])
        if item['response-time'] != '-':
            avg_time += int(item['response-time'])
            count += 1
    avg_time = avg_time/count
    pattern = '%d.%m.%Y %H:%M'
    etime = int(time.mktime(time.strptime(TIME, pattern)))
    return etime, bytes, getcount, postcount, visits, requests, avg_time

def batch_prepare(batch, fields):

    batch_statement = session.prepare("INSERT INTO ASIA_TRAFFIC(id, epoch_time, byte_transfer, get_count, post_count, visits, requests, average_response_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    batch.add(batch_statement, [uuid.uuid1(), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]])
    return 1

def insert_stats(batch):
    
    t_ins = datetime.now()
    session.execute(batch)
    print "Insertion time: %s  Time Elasped: %s" % ((datetime.now() - t_ins),(datetime.now() - t_init))
    return 1

if __name__ == '__main__':

    session.execute("CREATE TABLE ASIA_TRAFFIC(id uuid, epoch_time bigint, byte_transfer bigint, get_count bigint, post_count bigint, visits bigint, requests bigint, average_response_time bigint, PRIMARY KEY (id))")
    #function call
    spark_config()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *
from datetime import datetime
import time
from cassandra.query import BatchStatement
import uuid
global session, column_fam, key_space, t_init
t_init = datetime.now()
# connecting to the cassandra database to create a table
key_space = 'ASIA_KS'
column_fam = 'ASIA_CF'
session = Cluster(contact_points=['127.0.0.1'], port=9042).connect(keyspace=key_space)

def spark_config():

    # configuring spark with cassandra keyspace and columnfamily
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    RDD = sc.cassandraTable(key_space, column_fam)
    return start_connection(RDD)

def start_connection(rdd):

    time_rdd = rdd.select("timestamp","key").groupByKey().collect()
    pool = ConnectionPool(key_space, ['localhost:9160'], timeout=60)
    col_fam = ColumnFamily(pool, column_fam)
    batch = BatchStatement()
    row_count = 1
    for timekey in time_rdd:
        # function calls
        stats = retrieve_stats(timekey[1], timekey[0], col_fam)
        batch_prepare(batch, stats)
        if row_count % 300 == 0:
            # inserting in batches of 300
            insert_stats(batch)
            # creating a fresh batch
            batch = BatchStatement()
        row_count = row_count + 1
    insert_stats(batch)
    return 1

def retrieve_stats(keys, TIME, cass_conn):

    t1 = datetime.now()
    keytemp = []
    visits, requests, bytes, getcount, postcount, avg_time, count = 0,0,0,0,0,0,0
    for key in keys:
        keytemp.append(str(key))
    # creating an ordered dictionary containing log data retrieved from column_family
    log = cass_conn.multiget(keytemp)
    for item in log.values():
        # calculating total visits, requests, GET requests, POST requests, bytes and response-time for every timestamp
        if item['host'] != '-':
            visits += 1
        if item['request-type'] != '-':
            requests += 1
        if item['request-type'] == "GET":
            getcount += 1
        if item['request-type'] == "POST":
            postcount += 1
        if item['byte-transfer'] != '-':
            bytes += int(item['byte-transfer'])
        if item['response-time'] != '-':
            avg_time += int(item['response-time'])
            count += 1
    avg_time = avg_time/count
    pattern = '%d.%m.%Y %H:%M'
    etime = int(time.mktime(time.strptime(TIME, pattern)))
    return etime, bytes, getcount, postcount, visits, requests, avg_time

def batch_prepare(batch, fields):

    batch_statement = session.prepare("INSERT INTO ASIA_TRAFFIC(id, epoch_time, byte_transfer, get_count, post_count, visits, requests, average_response_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
    batch.add(batch_statement, [uuid.uuid1(), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]])
    return 1

def insert_stats(batch):

    t_ins = datetime.now()
    session.execute(batch)
    print "Insertion time: %s  Time Elasped: %s" % ((datetime.now() - t_ins),(datetime.now() - t_init))
    return 1

if __name__ == '__main__':

    session.execute("CREATE TABLE ASIA_TRAFFIC(id uuid, epoch_time bigint, byte_transfer bigint, get_count bigint, post_count bigint, visits bigint, requests bigint, average_response_time bigint, PRIMARY KEY (id))")
    #function call
    spark_config()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)