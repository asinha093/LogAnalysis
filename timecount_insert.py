'''
This file groups the timestamps and keys in the main columnfamily, calculates the counts of other fields present in the table and
inserts these counts to a new cassandra table: time_counts. So the new table time_counts now contains a list of every other field and
their respective counts for every unique timestamp!
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from pathos.pools import ProcessPool as Pool
import uuid

class initialize(object):

    def __init__(self, keyspace, source, dest, sc, host, thriftport, port, db):
        # initializing the class variables
        self.keyspace = keyspace
        self.source = source
        self.dest = dest
        self.sc = sc
        self.host = host
        self.thriftport = thriftport
        self.port = port
        self.db = db

    def create_table(self, dest, session):

        session.execute("CREATE TABLE IF NOT EXISTS "+dest+"(id uuid, timestamp varchar, ip list<varchar>, ip_count list<int>, requesttype list<varchar>, requesttype_count list<int>, requestlink list<varchar>, requestlink_count list<int>, response list<varchar>, response_count list<int>, virtualmachine list<varchar>, virtualmachine_count list<int>, byte_transfer bigint, response_time int, unique_visits int, total_visits int, last_timeid int, PRIMARY KEY (id))")
        return 1

    def get_key(self, db, session):

        query = "SELECT id, last_timeid FROM "+ db
        try:
            getdata = session.execute(query)
            for data in getdata:
                time_key = data[1]
        except:
            time_key = 1
        return time_key


    def get_timestamp(self,session,table,key):
        
        query = "SELECT timestamp FROM "+ table +" WHERE logid="+ str(key)
        data = session.execute(query)
        return data[0]

    def initialize_connection(self):
    
        cluster = self.host+':'+self.thriftport
        pool = ConnectionPool(self.keyspace, [cluster], timeout=30)
        col_fam = ColumnFamily(pool, self.source)
        session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        session.default_timeout=30
        # configuring spark with cassandra keyspace and columnfamily
        rdd = self.sc.cassandraTable(self.keyspace, self.source).cache()
        val = self.get_key(self.db, session)
        if val == 1:
            time_rdd = rdd.select("timestamp", "key").groupByKey().collect() # collecting rows in rdd grouped by timestamp
        else:
            time = self.get_timestamp(session,self.dest,val)
            time_rdd = rdd.select("timestamp", "key").filter(lambda row: row.timestamp > time).groupByKey().collect()
        # function call
        self.create_table(self.dest, session)
        batch = BatchStatement() # preparing a batchstatement
        row_count = 1
        for timekey in time_rdd:
            print row_count
            # function calls
            process = retrieve_count(timekey[1], timekey[0], col_fam) # create instance for the class retrieve_count
            fields = process.retrieve_fields()
            insert = batch_insert(batch, fields, self.dest, session) # create instance for the class batch_insert
            batches = insert.batch_prepare()
            if row_count % 100 == 0:
                # inserting in batches of 100
                insert.insert_fields()
                batch = BatchStatement() # creating a fresh batch
            row_count = row_count + 1
        insert.insert_fields() # inserting the final batch
        return 1

class retrieve_count(object):

    def __init__(self, keys, time, cass_conn):

        self.keys = keys
        self.time = time
        self.cass_conn = cass_conn

    def unique_count(self, set):

        value = []
        count = []
        # creating list value which contains the self.host/request/statuscode names and a list count containing their respective counts for a particular timestamp   
        for index in range(0,len(set)):
            if set[index] not in value:
                value.append(set[index].encode('utf-8')), count.append(set.count(set[index]))
                continue
        return value, count

    def retrieve_fields(self):

        ip, reqlink, reqtype, response, virtualm, keytemp = [], [], [], [], [], []
        bytes, avg_time, count, uniq_vis, total_vis = 0, 0, 0, 0, len(self.keys)
        for key in self.keys:
            keytemp.append(str(key))
        # creating an ordered dictionary containing log data retrieved from column_family
        log = self.cass_conn.multiget(keytemp)
        # starting a pool of 5 worker processes
        pool = Pool()
        pool.ncpus = 5
        for item in log.values():
            # appending lists with their respective values   
            ip.append(item['host']), reqlink.append(item['request_link']), reqtype.append(item['request_type']), response.append(str(item['response_code'])), virtualm.append(item['virtual_machine'])     
            if item['byte_transfer'] != '-':
                bytes += item['byte_transfer']
            if item['response_time'] != '-':
                avg_time += item['response_time']
                count += 1
        avg_time = avg_time/count
        # using the pool of workers to get results
        results = pool.map(self.unique_count, [ip, reqtype, reqlink, response, virtualm])
        pool.close()
        pool.join()
        uniq_vis = len(results[0][0])
        return self.time, results[0][0], results[0][1], results[1][0], results[1][1], results[2][0], results[2][1], results[3][0], results[3][1], results[4][0], results[4][1], bytes, avg_time, uniq_vis, total_vis

class batch_insert(object):

    def __init__(self, batch, fields, dest, session):

        self.batch = batch
        self.fields = fields
        self.dest = dest
        self.session = session           

    def batch_prepare(self):

        batch_statement = self.session.prepare("INSERT INTO "+self.dest+"(id, timestamp, ip, ip_count, requesttype, requesttype_count, requestlink, requestlink_count, response, response_count, virtualmachine, virtualmachine_count, byte_transfer, response_time, unique_visits, total_visits) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        self.batch.add(batch_statement, [uuid.uuid1(), self.fields[0], self.fields[1], self.fields[2], self.fields[3], self.fields[4], self.fields[5], self.fields[6], self.fields[7], self.fields[8], self.fields[9], self.fields[10], self.fields[11], self.fields[12], self.fields[13], self.fields[14]])
        return self.batch

    def insert_fields(self):

        self.session.execute(self.batch)
        return 1