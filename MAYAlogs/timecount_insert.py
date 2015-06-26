from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pyspark import SparkConf, SparkContext
from pyspark_cassandra.context import *
from cassandra.cluster import Cluster
from datetime import datetime
import multiprocessing
import uuid
global session, column_fam, key_space, t_init

t_init = datetime.now()
# connecting to the cassandra database to create a table
key_space = 'main'
column_fam = 'parsed_data'
session = Cluster(contact_points=['127.0.0.1'], port=9042).connect(keyspace=key_space)

def spark_config():

    # configuring spark with cassandra keyspace and columnfamily
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    RDD = sc.cassandraTable(key_space, column_fam)
    return start_connection(RDD)

def start_connection(rdd):

    # collecting rows in rdd grouped by key
    time_rdd = rdd.select("timestamp","key").groupByKey().collect()
    pool = ConnectionPool(key_space, ['localhost:9160'], timeout=60)
    col_fam = ColumnFamily(pool, column_fam)
    session.execute("CREATE TABLE time_counts (id uuid, timestamp varchar, ip list<varchar>, ip_count list<int>, requesttype list<varchar>, requesttype_count list<int>, requestlink list<varchar>, requestlink_count list<int> response list<varchar>, response_count list<int>, virtualmachine list<varchar>, virtualmachine_count list<int>, byte_transfer bigint, response_time varchar, unique_visits int, total_visits int PRIMARY KEY (id))")
    # function call"
    row_count = 1
    # preparing a batchstatement
    batch = BatchStatement()
    for timekey in time_rdd:
        print "Row: %s" % row_count
        # function calls
        stats = retrieve_stats(timekey[1], timekey[0], col_fam)
        batch_prepare(batch, stats)
        if row_count % 500:
            # inserting in batches of 1000
            insert_stats(batch)
            # creating a fresh batch
            batch= BatchStatement()
        row_count = row_count + 1
    insert_stats(batch)
    return 1

def retrieve_stats(keys, time, cass_conn):

    ip, reqlink, reqtype, response, virtualm, keytemp = [], [], [], [], [], []
    bytes, avg_time, count, uniq_vis, total_vis = 0, 0, 0, 0, len(keys)
    # starting a pool of 5 worker processes
    pool = multiprocessing.Pool(processes=5)
    for key in keys:
        keytemp.append(str(key))
    # creating an ordered dictionary containing log data retrieved from column_family
    log = cass_conn.multiget(keytemp)
    for item in log.values():
        # appending lists with their respective values
        ip.append(item['host']), reqlink.append(item['request-link']), reqtype.append(item['request-type']), response.append(item['response-code']), virtualm.append(item['virtual-machine'])
        if item['byte-transfer'] != '-':
            bytes += int(item['byte-transfer'])
        if item['response-time'] != '-':
            avg_time += int(item['response-time'])
            count += 1
    avg_time = avg_time/count
    # using the pool of workers to get results
    results = pool.map(unique_count, [ip, reqlink, reqtype, response, virtualm])
    pool.close()
    pool.join()
    uniq_vis = len(results[0][0])
    return time, results[0][0], results[0][1], results[1][0], results[1][1], results[2][0], results[2][1], results[3][0], results[3][1], results[4][0], results[4][1] bytes, avg_time, uniq_vis, total_vis

def unique_count(set):

    value = []
    count = []
    # creating list value which contains the host/request/statuscode names and a list count containing their respective counts for a particular timestamp
    for index in range(0,len(set)):
        if set[index] not in value:
            value.append(set[index].encode("utf-8")), count.append(set.count(set[index]))
            continue
    return value, count

def batch_prepare(batch, fields):

    batch_statement = session.prepare("INSERT INTO time_counts (id, timestamp, ip, ip_count, requesttype, requesttype_count, requestlink, requestlink_count, response, response_count , virtualmachine, virtualmachine_count, byte_transfer, response_time, unique_visits, total_visits) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    batch.add(batch_statement, [uuid.uuid1(), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[11], fields[13], fields[14]])
    return 1

def insert_fields(batch):

    session.execute(batch)
    print "Insertion time: %s  Time Elasped: %s" % ((datetime.now() - t_ins),(datetime.now() - t_init))
    return 1

if __name__ == '__main__':

    #function call
    spark_config()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)