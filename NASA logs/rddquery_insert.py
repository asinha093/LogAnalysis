from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

import pyspark_cassandra
from pyspark import SparkConf, SparkContext
from pyspark_cassandra.context import *
from cassandra.cluster import Cluster

from datetime import datetime

def spark_config():
    
    # configuring spark with cassandra keyspace and columnfamily
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable("main_keyspace", "main_column")
    # collecting rows in rdd grouped by key
    time_rdd = rdd.select("timestamp","key").groupByKey().take(50000)
    # function call 
    return start_connection(time_rdd)

def start_connection(time_rdd):

    pool = ConnectionPool('main_keyspace',['localhost:9160'])
    col_fam = ColumnFamily(pool,'main_column')
    # creating a dictionary to store counts and values according to timestamp
    timedict={}
    uuid = 100000
    row_count = 1
    for timekey in time_rdd:
        print "Row: %s" % row_count
        # function call --> adding values to the dictionary as follows: 
        timedict[timekey[0]] = retrieve_stats(col_fam, timekey[1])
        # function call
        insert_stats(timedict, timekey, uuid)
        uuid = uuid + 1
        row_count = row_count + 1
    return 1

def retrieve_stats(cass_con, keys):

    ip = []
    request = []
    scode = []
    keytemp = []

    for key in keys:
        keytemp.append(str(key))
    # creating an ordered dictionary containing log data retrieved from column_family
    log = cass_con.multiget(keytemp)
    for item in log.values():
        # appending lists with their respective values
        ip.append(item['host']), request.append(item['requestline']), scode.append(item['statuscode'])

    # function calls
    host_ = counts(ip)
    request_ = counts(request)
    scode_ = counts(scode)
    # returning the values and counts of hosts, requests and statuscodes at every timestamp to the time-dictionary 
    return host_[0], host_[1], request_[0], request_[1], scode_[0], scode_[1]

def counts(set):
    
    value = []
    count = []
    # creating list value which contains the host/request/statuscode names and a list count containing their respective counts for a particular timestamp   
    for index in range(0,len(set)):
        if set[index] not in value:
            value.append(set[index].encode("utf-8")), count.append(set.count(set[index]))
            continue
    return value, count

def insert_stats(timedict, timekey, uuid):

    t_ins = datetime.now()
    print type(uuid), type(timekey[0]), type(timedict[timekey[0]][0]), type(timedict[timekey[0]][1]), type(timedict[timekey[0]][2]), type(timedict[timekey[0]][3]), type(timedict[timekey[0]][4]), type(timedict[timekey[0]][5])
    session.execute("""INSERT INTO counts(uid, timestamp, ip, ipcount, request, requestcount, status, statuscount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",(uuid, str(timekey[0]), timedict[timekey[0]][0], timedict[timekey[0]][1], timedict[timekey[0]][2], timedict[timekey[0]][3], timedict[timekey[0]][4], timedict[timekey[0]][5]))
    print "Time for inserting: %s  Time Elapsed: %s" %( (datetime.now() - t_ins), (datetime.now() - t_init) )
    return 1

if __name__ == "__main__":

    global session, t_init
    t_init = datetime.now()
    # connecting to the cassandra database to create a table
    session = Cluster(contact_points=['127.0.0.1'], port=9042).connect(keyspace='m123') 
    session.execute("""CREATE TABLE counts( uid bigint, timestamp varchar, ip list<varchar>, ipcount list<int>, request list<varchar>, requestcount list<int>, status list<varchar>, statuscount list<int>, PRIMARY KEY (uid) )""")
    #function call
    spark_config()
    print "Total Time Elapsed: %s" % (datetime.now() - t)