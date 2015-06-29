from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *
def update_cassandra(conn,val, keys,table,temp):
    tid = val
    if temp == 0:
        col_fam = ColumnFamily(conn,table)
        for key in keys:
            qwe = col_fam.insert(str(key), {"logid":  tid})
            tid += 1
    if temp == 1:
        prepared_stmt = conn.prepare("UPDATE "+table+" SET timeid = ? WHERE (uid = ?)")
        for key in keys:
            bound_stmt = prepared_stmt.bind([tid, key])
            stmt = conn.execute(bound_stmt, parameters=[tid, key])
            tid += 1
    return tid

def get_key(db, session):
    query = "SELECT uid,last_timeid,last_logid FROM "+ db
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

def update_key(session,db,log_key,time_key,rowid):
    query = "UPDATE "+db+" SET last_timeid=%s, last_logid=%s WHERE uid=%s "
    data = session.execute(query,parameters=[time_key,log_key,rowid])
    return 1


def sorted_keys(spark_context,conn,keyspace ,val,table,temp):
    rdd = spark_context.cassandraTable(keyspace, table).cache()
    keys = []
    if temp == 1:
        sort_rdd = rdd.select("timestamp","uid").sortByKey(1,1).collect()
        for x in sort_rdd:
            keys.append(x['uid'])
    else:
        sort_rdd = rdd.select("timestamp","key").sortByKey(1,1).collect()
        for x in sort_rdd:
            keys.append(x['key'])

    rdd.unpersist()
    return update_cassandra(conn,val,keys,table,temp)



def main(cluster,keyspace, logdata,timedata, uidata ):
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    cass_cql = Cluster()
    session = cass_cql.connect(keyspace)
    cass_cli = ConnectionPool(keyspace,[cluster])
    val = get_key(uidata)
    log_key = sorted_keys(sc,cass_cli,keyspace,val[0],logdata,0)
    time_key = sorted_keys(sc,session,keyspace,val[1],timedata,1)
    update_key(log_key,time_key,val[2])


if __name__ == '__main__':
    main('localhost:9160',"main", "parsed_data_cli", "time_counts", "main_counts")