from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

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
    time_rdd = rdd.select("timestamp","key").groupByKey().collect()
    pool = ConnectionPool(key_space, ['localhost:9160'], timeout=60)
    col_fam = ColumnFamily(pool, column_fam)
    uuid = 100000
    # function calls --> filtering ip addresses and request links with response codes in 400 and 500 series (errors)
    error_resp1 = retrieve_errors(rdd.filter(lambda row: row['response-code'] >= '400').filter(lambda row: row['response-code'] < '500'))
    error_resp2 = retrieve_errors(rdd.filter(lambda row: row['response-code'] >= '500'))
    # function call
    insert_errors(uuid, error_resp1[0], error_resp1[1], error_resp2[0], error_resp1[1])
    print "Done inserting errors"
    #row_count = 1
    for timekey in time_rdd:
        #print "Row: %s" % row_count
        # function calls
        retrieve_stats(time_rdd, timekey[1], timekey[0], uuid, col_fam)
        uuid = uuid + 1
        #row_count = row_count + 1
    return 1

def retrieve_errors(rdd):

    ip_count = rdd.map(lambda row: row["host"]).collect()
    reqlink_count = rdd.map(lambda row: row["request-link"]).collect()
    rdd.unpersist()
    return ip_count, reqlink_count

def retrieve_stats(rdd, keys, time, uid, cass_conn):

    ip, request_type, request_prot, request_file, response_code, keytemp = [], [], [], [], [], []
    bytes = 0
    for key in keys:
        keytemp.append(str(key))
    # creating an ordered dictionary containing log data retrieved from column_family
    log = cass_conn.multiget(keytemp)
    for item in log.values():
        # appending lists with their respective values
        ip.append(item['host']), request_type.append(item['request-type']), request_file.append(item['request-file']), request_prot.append(item['request-protocol']), response_code.append(item['response-code'])
        if item['byte-transfer'] != '-':
            bytes += int(item['byte-transfer'])
    # function calls
    host = counts(ip)
    req_type = counts(request_type)
    req_prot = counts(request_prot)
    req_file = counts(request_file)
    resp = counts(response_code)
    uniq_vis = len(host[0])
    return insert_stats(uid, time, host[0], host[1], req_type[0], req_type[1], req_file[0], req_file[1], req_prot[0], req_prot[1], resp[0], resp[1], bytes, uniq_vis)

def counts(set):
    
    value = []
    count = []
    # creating list value which contains the host/request/statuscode names and a list count containing their respective counts for a particular timestamp   
    for index in range(0,len(set)):
        if set[index] not in value:
            value.append(set[index].encode("utf-8")), count.append(set.count(set[index]))
            continue
    return value, count

def insert_errors(num, ip400, link400, ip500, link500):

    t_ins = datetime.now()
    session.execute("""INSERT INTO NASA_ERROR(uid, ip_400, request_link_400, ip_500, request_link_500) VALUES (%s, %s, %s, %s, %s)""",(num, ip400, link400, ip500, link500))
    print "Time for inserting: %s  Time Elapsed: %s" %( (datetime.now() - t_ins), (datetime.now() - t_init) )
    return 1

def insert_stats(num, time, ip, ip_count, reqtype, reqtype_count, reqfile, reqfile_count, reqprot, reqprot_count, resp, resp_count, bytes, uniq_vis):

    t_ins = datetime.now()
    session.execute("""INSERT INTO NASA_COUNT(uid, timestamp, ip, ip_count, request_type, request_type_count, request_file, request_file_count, request_protocol, request_protocol_count, response_code, response_code_count, byte_transfer, unique_visits) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",(num, time, ip, ip_count, reqtype, reqtype_count, reqfile, reqfile_count, reqprot, reqprot_count, resp, resp_count, bytes, uniq_vis))
    print "Time for inserting: %s  Time Elapsed: %s" %( (datetime.now() - t_ins), (datetime.now() - t_init) )
    return 1

if __name__ == "__main__":

    session.execute("""CREATE TABLE NASA_COUNT ( uid bigint, timestamp varchar, ip list<varchar>, ip_count list<int>, request_type list<varchar>, request_type_count list<int>, request_file list<varchar>, request_file_count list<int>, request_protocol list<varchar>, request_protocol_count list<int>, response_code list<varchar>, response_code_count list<int>, byte_transfer bigint, unique_visits int, PRIMARY KEY (uid) )""")
    session.execute("""CREATE TABLE NASA_ERROR ( uid bigint, ip_400 list<varchar>, request_link_400 list<varchar>, ip_500 list<varchar>, request_link_500 list<varchar>, PRIMARY KEY (uid) )""")
    #function call
    spark_config()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)