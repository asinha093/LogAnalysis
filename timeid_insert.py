'''
This file retieves the last_timeid column in the table main_count and inserts the new time_ids starting from the retrieved value in the table time_counts 
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster

class initialize(object):

    def __init__(self, keyspace, timedata, uidata, sc, host, thriftport, port):
        # initializing the class variables
        self.keyspace = keyspace
        self.timedata = timedata
        self.uidata = uidata
        self.sc = sc
        self.host = host
        self.thriftport = thriftport
        self.port = port

    def update_key(self, session, dest, time_key, rowid):
        # updates the last timeid in the table main_count
        session.execute("UPDATE "+dest+" SET last_timeid=%s WHERE id=%s", parameters=[time_key, rowid])
        return 1

    def initialize_connection(self):

        cluster = self.host+':'+self.thriftport
        session = Cluster(contact_points=[self.host], port=self.port).connect(self.keyspace)
        # create instance for the class sort_insert_key
        sort = sort_insert_key()
        # function call
        val = sort.get_key(self.uidata, session) 
        # function calls
        time_key = sort.sorted_keys(self.sc, session, self.keyspace, val[0], self.timedata)
        self.update_key(session, self.uidata, time_key, val[1])
        return 1

class sort_insert_key(object):

    def __init__(self):

        pass

    def get_key(self, dest, session):
        # retrieves the id, and last_timeid (if present) representing the only row in the table main_count
        query = "SELECT id, last_timeid FROM "+ dest
        getdata = session.execute(query)
        for data in getdata:
            time_key = data[1]
            row_key = data[0]
        # when there is no value inserted in the column last_timeid in the table main_count    
        if time_key == None:
            time_key = 1
        return time_key, row_key


    def get_timestamp(self, session, table, key):
        # retrieves the timestamp associated with the last_timeid in the table time_counts i.e the last updated timestamp
        query = "SELECT timestamp FROM "+ table +" WHERE last_timeid="+ str(key)
        data = session.execute(query)
        return data[0]

    def sorted_keys(self, spark_context, conn, keyspace, val, table):

        rdd = spark_context.cassandraTable(keyspace, table)
        keys = []
        if val == 1:
            sort_rdd = rdd.select("timestamp", "id").sortByKey(1,1).collect() # sort timestamp with key (timeid) in ascending order
        else:
            time = self.get_timestamp(conn,table, val)
            sort_rdd = rdd.select("timestamp","id").filter(lambda row: row.timestamp > time).sortByKey(1,1).collect()

        i = 1
        for item in sort_rdd:
            keys.append(item['id'])
            print i
            i+=1
        rdd.unpersist()
        return self.update_cassandra(conn, val, keys, table)

    def update_cassandra(self, conn, val, keys, table):

        t_id = val
        print t_id
        # updating the column last_timeid with the increasing values of t_id
        for key in keys:
            conn.execute("UPDATE "+table+" SET last_timeid=%s WHERE id =%s", parameters=[t_id, key])
            print t_id
            t_id += 1
        return t_id