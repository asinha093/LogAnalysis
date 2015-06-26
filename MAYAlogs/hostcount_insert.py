__author__ = 'rahul'

from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster, BatchStatement
from pyspark import SparkConf
from pyspark_cassandra.context import *
import datetime
import uuid

def create_table(session,table):
    session.execute("CREATE TABLE "+table+" (id uuid, timestamp varchar, ip list<varchar>, ip_count list<int>, requesttype list<varchar>, requesttype_count list<int>, requestlink list<varchar>, requestlink_count list<int> response list<varchar>, response_count list<int>, virtualmachine list<varchar>, virtualmachine_count list<int>, byte_transfer bigint, response_time varchar, unique_visits int, total_visits int PRIMARY KEY (id))")session.execute("CREATE TABLE time_counts (id uuid, timestamp varchar, ip list<varchar>, ip_count list<int>, requesttype list<varchar>, requesttype_count list<int>, requestlink list<varchar>, requestlink_count list<int> response list<varchar>, response_count list<int>, virtualmachine list<varchar>, virtualmachine_count list<int>, byte_transfer bigint, response_time varchar, unique_visits int, total_visits int PRIMARY KEY (id))")
    return 1

def batch_prepare(batch, fields,dest,session):
    batch_statement = session.prepare("INSERT INTO " +dest+"(uid, host, links, user_agent, byte_transfer, response_time) VALUES (?, ?, ?, ?, ?, ?)")
    batch.add(batch_statement, [uuid.uuid1(), fields[0], fields[1], fields[2], fields[3], fields[4]])
    return batch

def insert_fields(batch,session):
    session.execute(batch)
    return 1


def retrv_stats(keys, host,source_conn):
    links, vir_mem ,reqtype, userdata = [], [], [], []
    bytes, avg_time, count, unique_visits = 0, 0, 0, 0
    temp1 = []
    for x in keys:
        temp1.append(str(x))
    data = source_conn.multiget(temp1)
    for i in data.values():
        links.append(i["reqtlink"])
        userdata.append(i['user_data'])
        if i['byte_transfer'] != '-':
            bytes += int(i['byte_transfer'])
        if i['response_time'] != '-':
            avg_time += int(i['response_time'])
            count += 1
    avg_time = avg_time/count
    return host, links, userdata, bytes, avg_time

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


def initilize_conn(cluster, keyspace,source,dest):
    pool = ConnectionPool(keyspace,[cluster])
    col_fam = ColumnFamily(pool,source)
    cluster = Cluster()
    session = cluster.connect(keyspace)
    create_table(session,dest)
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable(keyspace, source).cache()
    time_sort = rdd.select("host","key").groupByKey().collect()
    batch = BatchStatement()
    row_count=0
    for x in time_sort:
        t = datetime.datetime.now()
        fields = retrv_stats(x[1], x[0],col_fam)
        batch = batch_prepare(batch, fields,dest,session)
        if row_count % 1000 == 0:
            insert_fields(batch,session)
            batch = BatchStatement()
        elif row_count == len(time_sort):
            # inserting the final batch
            insert_fields(batch,session)


if __name__ == '__main__':
    initilize_conn('localhost:9160','main',"parsed_data_cli","ipdata")



