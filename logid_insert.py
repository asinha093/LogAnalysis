__author__ = 'rahul'
'''
This file retrieves the location details of all the hosts in the table: ipdata.
The file extracts the country_codes associated with their respective ip addresses and updates them to the existing cassandra table: ipdata.
This file also prepares 2 lists: 1) a list containg all country_codes and 2) a list containing their respective counts, and inserts them
into the existing table: main_counts which will be used for building the UI !
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from pyspark import SparkConf, SparkContext
from pyspark_cassandra.context import *
from datetime import datetime

class initialize(object):

    def __init__(self, keyspace, logdata, uidata, sc, host, thriftport, port):
        # initializing the class variables
        self.keyspace = keyspace
        self.logdata = logdata
        self.uidata = uidata
        self.sc = sc
        self.host = host
        self.thriftport = thriftport
        self.port = port

    def update_key(self, session, dest, log_key, rowid):

        session.execute("UPDATE "+dest+" SET last_logid=%s WHERE id=%s", parameters=[log_key, rowid])
        return 1

    def initialize_connection(self):

        cluster = self.host+':'+self.thriftport
        #conf = SparkConf().set("spark.cassandra.connection.host", self.host).set("spark.cassandra.connection.native.port",str(self.port))
        #sc = CassandraSparkContext(conf=conf)
        session = Cluster(contact_points=[self.host], port=self.port).connect(self.keyspace)
        cass_cli = ConnectionPool(self.keyspace, [cluster], timeout=60)
        # create instance for the class sort_insert_key
        sort = sort_insert_key()
        val = sort.get_key(self.uidata, session)
        print val
        log_key = sort.sorted_keys(self.sc, cass_cli, self.keyspace, val[0], self.logdata)
        self.update_key(session, self.uidata, log_key, val[1])
        return 1

class sort_insert_key(object):

    def __init__(self):

        pass

    def get_key(self, dest, session):

        query = "SELECT id, last_logid FROM "+ dest
        getdata = session.execute(query)
        for data in getdata:
            log_key = data[1]
            row_key = data[0]
        if log_key == None:
            log_key = 1
        return log_key, row_key

    def sorted_keys(self, spark_context, conn, keyspace, val, table):

        rdd = spark_context.cassandraTable(keyspace, table)
        keys = []
        sort_rdd = rdd.select("timestamp", "key").sortByKey(1,1).collect() # sort timestamp with key (timeid) in ascending order
        i = 1
        for item in sort_rdd:
            keys.append(item['key'])
            print i
            i+=1
        rdd.unpersist()
        return self.update_cassandra(conn, val, keys, table)

    def update_cassandra(self, conn, val, keys, table):

        l_id = val
        print l_id
        # updating the column last_logid with the increasing values of l_id
        col_fam = ColumnFamily(conn, table)
        for key in keys:
            col_fam.insert(str(key), {"logid": l_id})
            print l_id
            l_id += 1
        return l_id