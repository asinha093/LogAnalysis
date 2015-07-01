__author__ = 'abhinav'
'''
This file calculates the counts of specific fields present in the table such as total requests, total bytes transfered, etc and
inserts these counts to a new cassandra table: traffic. This table will be used to do predictive analysis and forecast these counts
for the future! 
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext
from pyspark_cassandra.context import *
from cassandra.query import BatchStatement
from datetime import datetime
import time
import uuid

class initialize(object):

    def __init__(self, keyspace, source, dest, sc, host, thriftport, port):
        # initializing the class variables
        self.keyspace = keyspace
        self.source = source
        self.dest = dest
        self.sc = sc
        self.host = host
        self.thriftport = thriftport
        self.port = port

    def create_table(self, session, dest):

        session.execute("CREATE TABLE IF NOT EXISTS "+dest+"(id uuid, epoch_time bigint, byte_transfer bigint, get_count bigint, post_count bigint, visits bigint, requests bigint, average_response_time bigint, PRIMARY KEY (id))")
        return 1

    def initialize_connection(self):

        cluster = self.host+":"+self.thriftport
        pool = ConnectionPool(self.keyspace, [cluster], timeout=30)
        col_fam = ColumnFamily(pool, self.source)
        session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        # configuring spark with cassandra keyspace and columnfamily
        #conf = SparkConf().set("spark.cassandra.connection.host", self.host).set("spark.cassandra.connection.native.port",str(self.port))
        #sc = CassandraSparkContext(conf=conf)
        rdd = self.sc.cassandraTable(self.keyspace, self.source)
        time_rdd = rdd.select("timestamp", "key").groupByKey().collect() # collecting rows in rdd grouped by timestamp
        # function call
        self.create_table(session, self.dest)
        batch = BatchStatement() # preparing a batchstatement
        row_count = 1
        process = retrieve_insert() # create instance for the class retrieve_insert
        insert = batch_insert() # create instance for the class batch_insert
        for timekey in time_rdd:
            print row_count
            # function calls
            fields = process.retrieve_fields(timekey[1], timekey[0], col_fam)
            batches = insert.batch_prepare(batch, fields, self.dest, session)
            if row_count % 300 == 0:
                # inserting in batches of 300
                insert.insert_fields(batches, session)
                batch = BatchStatement() # creating a fresh batch
            row_count = row_count + 1
        insert.insert_fields(batches, session) # inserting the final batch
        return 1

class retrieve_insert(object):

    def __init__(self):

        pass

    def retrieve_fields(self, keys, TIME, cass_conn):

        keytemp = []
        visits, requests, bytes, getcount, postcount, avg_time, count = 0, 0, 0, 0, 0, 0, 0
        for key in keys:
            keytemp.append(str(key))
        # creating an ordered dictionary containing log data retrieved from column_family
        log = cass_conn.multiget(keytemp)
        for item in log.values():
            # calculating total visits, requests, GET requests, POST requests, bytes and response-time for every timestamp
            if item['host'] != '-':
                visits += 1
            if item['request_type'] != '-':
                requests += 1
            if item['request_type'] == "GET":
                getcount += 1
            if item['request_type'] == "POST":
                postcount += 1
            if item['byte_transfer'] != '-':
                bytes += int(item['byte_transfer'])
            if item['response_time'] != '-':
                avg_time += int(item['response_time'])
                count += 1
        avg_time = avg_time/count
        pattern = '%d.%m.%Y %H:%M'
        etime = int(time.mktime(time.strptime(TIME, pattern))) # converting timestam to epoch time 
        return etime, bytes, getcount, postcount, visits, requests, avg_time

class batch_insert(object):

    def __init__(self):

        pass

    def batch_prepare(self, batch, fields, dest, session):

        batch_statement = session.prepare("INSERT INTO "+dest+"(id, epoch_time, byte_transfer, get_count, post_count, visits, requests, average_response_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        batch.add(batch_statement, [uuid.uuid1(), fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6]])
        return batch

    def insert_fields(self, batch, session):

        session.execute(batch)
        return 1