__author__ = 'rahul'
'''
This file retieves the last_logid column in the table main_count and inserts the new log_ids starting from the retrieved value in the table parsed_data 
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster

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
        # updates the last logid in the table main_count
        session.execute("UPDATE "+dest+" SET last_logid=%s WHERE id=%s", parameters=[log_key, rowid])
        return 1

    def initialize_connection(self):

        cluster = self.host+':'+self.thriftport
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
        # retrieves the id, and last_logid (if present) representing the only row in the table main_count
        query = "SELECT id, last_logid FROM "+ dest
        getdata = session.execute(query)
        print getdata
        for data in getdata:
            log_key = data[1]
            row_key = data[0]
        # when there is no value inserted in the column last_logid in the table main_count    
        if log_key== None:
            log_key = 1
        return log_key, row_key

    def get_timestamp(self, session, table, key):
        query = "SELECT timestamp FROM "+ table +" WHERE logid="+ str(key)
        data = session.execute(query)
        return data[0]

    def sorted_keys(self, spark_context, conn, keyspace, val, table):
        # retrieves the timestamp associated with the last_logid in the table parsed_data i.e the last updated timestamp
        rdd = spark_context.cassandraTable(keyspace, table)
        keys = []
        print val
        if val == 1:
            sort_rdd = rdd.select("timestamp", "key").sortByKey(1,1).collect() # sort timestamp with key in ascending order
        else:
            time = self.get_timestamp(conn,table, val)
            sort_rdd = rdd.select("timestamp","key").filter(lambda row: row.timestamp > time).sortByKey(1,1).collect()
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