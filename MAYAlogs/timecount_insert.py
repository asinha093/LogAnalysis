from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster, BatchStatement
from pyspark import SparkConf
from pyspark_cassandra.context import *
import multiprocessing
import datetime
import uuid
global col_fam,session

cluster = Cluster()
session = cluster.connect('main')
cass_client = ConnectionPool('main',['localhost:9160'])
col_fam = ColumnFamily(cass_client,'parsed_data_cli')


def insert_cassandra(timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, response, respcount, reqtype, reqtype_count, bytes, response_time, uniq_visit, tot_visit):
    uid1 = uuid.uuid1()
    t12 = datetime.datetime.now()
    qwe = session.execute("""INSERT INTO time_counts (uid, timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, response, respcount, reqtype, reqtype_count, byte_transfer, response_time, unique_visit, total_visit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",(uid1, timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, response, respcount, reqtype, reqtype_count, bytes, response_time, uniq_visit, tot_visit))
    print "insertion"
    print datetime.datetime.now() - t12
    return 1

def retrv_stats(keys, time):
    ip_add, links, vir_mem, response_code,reqtype = [], [], [], [], []
    bytes, avg_time, count, unique_visits, total_visits = 0, 0, 0, 0, len(keys)
    pool = multiprocessing.Pool(processes=5)
    #pool1 = Pool(processes=100)
    temp1 = []
    prepared_stmt = session.prepare ("SELECT * FROM parsed_data_cli WHERE (key = ?)")
    for x in keys:
        temp1.append(str(x))
    data = col_fam.multiget(temp1)
    for i in data.values():
        ip_add.append(i['host'])
        links.append(i['req_link'])
        vir_mem.append(i['virt_mach'])
        response_code.append(i['response'])
        reqtype.append(i['req_type'])
        if i['byte_transfer']:
            bytes += int(i['byte_transfer'])
        if i['response_time']:
            avg_time += int(i['response_time'])
            count += 1
    avg_time = avg_time/count
    temp = pool.map(unique_count, [ip_add, links, vir_mem, response_code, reqtype])
    pool.close()
    pool.join()
    unique_visits = len(temp[0][0])
    print temp[4][1], temp[4][0], bytes, avg_time, unique_visits
    return insert_cassandra( time, temp[0][0], temp[0][1], temp[1][0], temp[1][1], temp[2][0], temp[2][1], temp[3][0], temp[3][1], temp[4][0], temp[4][1], bytes, avg_time, unique_visits, total_visits)


def unique_count(set):
    temp = []
    temp2 = []
    for x in range(0,len(set)):
        if set[x] not in temp:
            temp.append(set[x])
            temp2.append(set.count(set[x]))
        else:
            continue
    return temp, temp2

def batch_prepare(batch, fields):

    batch_statement = session.prepare("INSERT INTO time_counts (uid, timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, response, respcount, reqtype, reqtype_count, byte_transfer, response_time, unique_visit, total_visit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    batch.add(batch_statement, [uuid.uuid1(), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10], fields[11], fields[11], fields[13], fields[14]])
    return 1

def insert_fields(batch):

    session.execute(batch)
    #print "Insertion time: %s  Time Elasped: %s" % ((datetime.now() - t_ins),(datetime.now() - t_init))
    return 1


def initilize_conn():
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable("main", "parsed_data_cli").cache()
    time_sort = rdd.select("timestamp","key").groupByKey().take(1000000)
    total = 0
    count = 0
    num = 100000
    t7 = datetime.datetime.now()
    #batch = BatchStatement()
    for x in time_sort:
        t = datetime.datetime.now()
        fields = retrv_stats(x[1], x[0])
        #batch_prepare(batch, fields)
        total += len(x[1])
        count += 1
        #if count % 100 == 0:
        #    # inserting in batches of 1000
        #    insert_fields(batch)
        ##    # creating a fresh batch
         #   batch = BatchStatement()
        #elif count == len(time_sort):
        #    # inserting the final batch
         #   insert_fields(batch)
        print total, count
        print datetime.datetime.now()- t, datetime.datetime.now() - t7
    print 'done'
if __name__ == '__main__':
    initilize_conn()



