__author__ = 'rahul'
'''
This file groups the hosts and keys in the main columnfamily, calculates the counts of other fields present in the table and
inserts these counts to a new cassandra table: ipdata. So the new table main_counts now contains a list of other fields and
their respective counts for every unique host!
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from datetime import datetime
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

    def create_table(self, dest, session):

        session.execute("CREATE TABLE IF NOT EXISTS "+dest+"(id uuid PRIMARY KEY, byte_transfer int, country varchar, host varchar, requestlink list<varchar>, response_time int, user_agent list<varchar>, last_timeid int, last_logid int)")
        return 1

    def initialize_connection(self):

        cluster = self.host+":"+self.thriftport
        pool = ConnectionPool(self.keyspace, [cluster], timeout=30)
        col_fam = ColumnFamily(pool, self.source)
        session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        rdd = self.sc.cassandraTable(self.keyspace, self.source).cache()  
        host_sort = rdd.select("host", "key").groupByKey().collect() # collecting rows in rdd grouped by host
        # function call
        self.create_table(self.dest, session)
        batch = BatchStatement() # preparing a batchstatement
        row_count = 1
        for hostkey in host_sort:
            print row_count       
            process = retrieve(hostkey[1], hostkey[0], col_fam) # create instance for the class retrieve  
            fields = process.retrieve_fields()
            insert = batch_insert(batch, fields, self.dest, session) # create instance for the class batch_insert
            batches = insert.batch_prepare()
            if row_count % 300 == 0:
                # inserting in batches of 300
                insert.insert_fields()
                batch = BatchStatement() # creating a fresh batch
            row_count+=1
        insert.insert_fields() # inserting the final batch
        return 1

class retrieve(object):

    def __init__(self, keys, host, cass_conn):

        self.keys = keys
        self.host = host
        self.cass_conn = cass_conn

    def retrieve_fields(self):

        req_link, useragent, keytemp = [], [], []
        bytes, avg_time, count = 0, 0, 0
        for key in self.keys:
            keytemp.append(str(key))
        # creating an ordered dictionary containing log data retrieved from column_family
        log = self.cass_conn.multiget(keytemp)
        for item in log.values():
            # appending lists with their respective values               
            req_link.append(item['request_link'].encode('utf-8'))
            useragent.append(item['user_agent'].encode('utf-8'))
            if item['byte_transfer'] != '-':
                bytes += int(item['byte_transfer'])
            if item['response_time'] != '-':
                avg_time += int(item['response_time'])
                count += 1
        avg_time = avg_time/count
        return self.host, bytes, req_link, avg_time, useragent

class batch_insert(object):

    def __init__(self, batch, fields, dest, session):

        self.batch = batch
        self.fields = fields
        self.dest = dest
        self.session = session

    def batch_prepare(self):

        batch_statement = self.session.prepare("INSERT INTO " +self.dest+"(id, host, byte_transfer, requestlink, response_time, user_agent) VALUES (?, ?, ?, ?, ?, ?)")
        self.batch.add(batch_statement, [uuid.uuid1(), self.fields[0], self.fields[1], self.fields[2], self.fields[3], self.fields[4]])
        return self.batch

    def insert_fields(self):

        self.session.execute(self.batch)
        return 1