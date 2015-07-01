__author__ = 'rahul'
'''
This file calculates the unique counts of various fields stored in the main columnfamily.
The file stores these counts (a list) into the existing table: main_counts which will be used for building the UI !
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pyspark import SparkConf, SparkContext
from pyspark_cassandra.context import *
from cassandra.cluster import Cluster
from datetime import datetime
import uuid

class initialize(object):

    def __init__(self, keyspace, source, dest, sc, host, port):
        # initializing the class variables
        self.keyspace = keyspace
        self.source = source
        self.dest = dest
        self.sc = sc
        self.host = host
        self.port = port

    def initilize_connection(self):

        #conf = SparkConf().set("spark.cassandra.connection.host", self.host).set("spark.cassandra.connection.native.port", str(self.port))
        #sc = CassandraSparkContext(conf=conf)
        rdd = self.sc.cassandraTable(self.keyspace, self.source)
        session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        vm_rdd = rdd.select("virtual_machine", "key").groupByKey().collect()  # collecting rows in rdd grouped by virtual_machine
        vm_list, vm_count = [], []
        for item in vm_rdd:
            vm_list.append(item[0]) # storing the virtual_machine name
            vm_count.append(len(item[1])) # storing the respective virtual machine's count
        reqtype_list, reqtype_count = [], []
        reqtype_rdd = rdd.select("request_type", "key").map(lambda r: (r["request_type"], 1)).reduceByKey(lambda a, b: a + b).collect() # collecting rows in rdd grouped by requesttype
        for item in reqtype_rdd:
            reqtype_list.append(item[0]) # storing the request_type
            reqtype_count.append(item[1]) # storing the respective request_type count
        os_list, os_count = [], []
        os_rdd = rdd.select("operating_system", "key").groupByKey().collect() # collecting rows in rdd grouped by operating_system
        for item in os_rdd:
            os_list.append(item[0]) # storing the os name
            os_count.append(len(item[1])) # storing the respective os count
        device_list, device_count = [], []
        device_rdd = rdd.select("device_type", "key").groupByKey().collect() # collecting rows in rdd grouped by device_type
        for item in device_rdd:
            device_list.append(item[0]) # storing the device name
            device_count.append(len(item[1])) # storing the respective device's count
        # create instance for the class insert_data
        insert = insert_data()
        return insert.insert_cassandra(self.dest, session, vm_list, vm_count, reqtype_list, reqtype_count, os_list, os_count, device_list, device_count)

class insert_data(object):

    def __init__(self):

        pass

    def insert_cassandra(self, dest, session, vm_list, vm_count, reqtype_list, reqtype_count, os_list, os_count, device_list, device_count):

        query = "SELECT id FROM "+dest
        uid = session.execute(query)[0][0]
        statement = "UPDATE "+dest+" SET vm_list=%s, vm_count=%s, req_list=%s, req_count=%s, os_list=%s, os_count=%s, device_list=%s, device_count=%s WHERE id=%s"
        session.execute(statement, parameters=[vm_list, vm_count, reqtype_list, reqtype_count, os_list, os_count, device_list, device_count, uid])    
        return 1