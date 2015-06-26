from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *

import multiprocessing
import datetime
global cass_client,session
pool = ConnectionPool('main',['localhost:9160'])
cluster = Cluster()
session = cluster.connect('main')
cass_client = ConnectionPool('main',['localhost:9160'])

def update_cassandra(val, keys,table):
    tid = val
    if table == "parsed_data_cli":
        col_fam = ColumnFamily(cass_client,table)
        for key in keys:
            qwe = col_fam.insert(str(key), {"timeid":  tid})
            tid += 1
    if table == "time_counts":
        prepared_stmt = session.prepare ("UPDATE time_counts SET timeid = ? WHERE (uid = ?)")
        for key in keys:
            bound_stmt = prepared_stmt.bind([tid, key])
            stmt = session.execute(bound_stmt, parameters=[tid, key])
            tid += 1
    return tid

def get_key():
    query = "SELECT uid,last_timeid,last_logid FROM main_counts"
    data = session.execute(query)
    for x in data:
        log_key = x[2]
        time_key = x[1]
        row_key = x[0]
    if log_key == None:
        key = 1
    if time_key == None:
        time_key = 1

    return log_key,time_key,row_key

def update_key(log_key,time_key,rowid):
    query = "UPDATE main_counts SET last_timeid=%s, last_logid=%s WHERE uid=%s "
    data = session.execute(query,parameters=[time_key,log_key,rowid])
    return 1


def sorted_keys(spark_context,val,table):
    rdd = spark_context.cassandraTable("main", table).cache()
    keys = []
    if table == "time_counts":
        sort_rdd = rdd.select("timestamp","uid").sortByKey(1,1).collect()
        for x in sort_rdd:
            keys.append(x['uid'])
    else:
        sort_rdd = rdd.select("timestamp","key").sortByKey(1,1).collect()
        for x in sort_rdd:
            keys.append(x['key'])

    rdd.unpersist()
    return update_cassandra(val,keys,table)



def main():
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    val = get_key()
    log_key = sorted_keys(sc,val[0],"parsed_data_cli")
    time_key = sorted_keys(sc,val[1],"time_counts")
    update_key(log_key,time_key,val[2])


if __name__ == '__main__':
    main()