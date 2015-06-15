from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf
from pyspark_cassandra.context import *
import multiprocessing
import datetime
global col_fam,session
pool = ConnectionPool('main',['localhost:9160'])
col_fam = ColumnFamily(pool,'random2')
cluster = Cluster()
session = cluster.connect('main')


def insert_cassandra(uid, timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, reponse, respcount, reqtype, reqtype_count, bytes, response_time, uniq_visit):
    qwe = session.execute("""INSERT INTO res_final2 (uid, timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, reponse, respcount, reqtype, reqtype_count, byte_transfer, response_time, unique_visit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",(uid, timestamp, ipadd, ipcount, link, linkcount, vm, vmcount, reponse, respcount, reqtype, reqtype_count, bytes, response_time, uniq_visit))
    return 1

def retrv_stats(keys, time, num):
    ip_add, links, vir_mem, response_code,reqtype = [], [], [], [], []
    bytes, avg_time, count, unique_visits = 0, 0, 0, 0
    pool = multiprocessing.Pool(processes=5)
    #pool1 = Pool(processes=100)
    temp1 = []
    for x in keys:
        temp1.append(str(x))
    data = col_fam.multiget(temp1)
    for i in data.values():
        ip_add.append(i['host'])
        links.append(i['reqtlink'])
        vir_mem.append(i['virt_mach'])
        response_code.append(i['response'])
        reqtype.append(i['reqtype'])
        if i['byte_transfer'] != '-':
            bytes += int(i['byte_transfer'])
        if i['response_time'] != '-':
            avg_time += int(i['response_time'])
            count += 1
    avg_time = avg_time/count
    temp = pool.map(unique_count, [ip_add, links, vir_mem, response_code, reqtype])
    pool.close()
    pool.join()
    unique_visits = len(temp[0][0])
    print temp[4][1], temp[4][0], bytes, avg_time, unique_visits
    return insert_cassandra(num, time, temp[0][0], temp[0][1], temp[1][0], temp[1][1], temp[2][0], temp[2][1], temp[3][0], temp[3][1], temp[4][0], temp[4][1], bytes, avg_time, unique_visits)


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



def initilize_conn():
    conf = SparkConf().set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.native.port","9042")
    sc = CassandraSparkContext(conf=conf)
    rdd = sc.cassandraTable("main", "random2").cache()
    time_sort = rdd.select("timestamp","key").groupByKey().take(1000000)
    total = 0
    num = 100000
    t7 = datetime.datetime.now()
    for x in time_sort:
        t = datetime.datetime.now()
        retrv_stats(x[1], x[0],num)
        total += len(x[1])
        num += 1
        print total
        print datetime.datetime.now()- t, datetime.datetime.now() - t7
    print 'done'
if __name__ == '__main__':
    initilize_conn()



