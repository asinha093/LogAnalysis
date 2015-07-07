'''
This file reads the logfile, parses various fields present in the logfile, creates a new keyspace in cassandra, creates a columnfamily in the keyspace with
the various fields as column names and adds data into the database!
'''
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import *
from pycassa.batch import *
from datetime import datetime, timedelta
import uuid

class initialize(object):

    def __init__(self, keyspace, columnfamily, file_name, host, port):
        # initializing the class variables
        self.keyspace = keyspace
        self.columnfamily = columnfamily
        self.file_name = file_name
        self.host = host
        self.port = port

    def create_keyspace(self, cluster):
        # this function creates the main keyspace in cassandra database --> keyspace name = self.keyspace
        # this function uses the pyCassa Shell commands
        sys = SystemManager(cluster)
        try:
            pool = ConnectionPool(self.keyspace,[cluster])
        except:
            sys.create_keyspace(self.keyspace, SIMPLE_STRATEGY, {'replication_factor': '1'})
            validators = {'timestamp': UTF8_TYPE,'request_type': UTF8_TYPE, 'request_link': UTF8_TYPE, 'request_details': UTF8_TYPE, 'response_code': UTF8_TYPE, 'byte_transfer': INT_TYPE, 'response_time': INT_TYPE, 'user_agent': UTF8_TYPE, 'virtual_machine': UTF8_TYPE, 'operating_system': UTF8_TYPE, 'device_type': UTF8_TYPE, 'host': UTF8_TYPE, 'logid': INT_TYPE}
            sys.create_column_family(self.keyspace, self.columnfamily, super=False, comparator_type=UTF8_TYPE, key_validation_class=UTF8_TYPE, column_validation_classes=validators)
            sys.create_index(self.keyspace, self.columnfamily, "host",UTF8_TYPE,index_type=KEYS_INDEX)
            sys.create_index(self.keyspace, self.columnfamily, "timestamp",UTF8_TYPE,index_type=KEYS_INDEX)
            sys.create_index(self.keyspace, self.columnfamily, "logid",INT_TYPE,index_type=KEYS_INDEX)
            sys.close()
        return 1

    def get_file(self):
        # connecting to the created columnfamily
        cluster = self.host+':'+self.port
        self.create_keyspace(cluster)
        cass_client = ConnectionPool(self.keyspace, [cluster], timeout=30)
        dest_conn = ColumnFamily(cass_client, self.columnfamily)
        text = open(self.file_name).readlines()
        # preparing a batchstatement
        batch = dest_conn.batch(queue_size=1000)
        # create instance for the class parse_insert
        process = parse_insert()
        row_count = 1
        for data in text:
            # extracting the logdata in every line
            fields = process.retrieve_fields(data)
            batches = process.insert_fields(batch, fields)
            if row_count % 100000 == 0:
                print "%s logs inserted" % row_count
            if row_count % 1000 == 0:
                # inserting in batches of 1000
                batches.send()
                # creating a fresh batch
                batch = dest_conn.batch(queue_size=1000)
            row_count = row_count + 1
        batches.send()
        print "%s logs inserted" % row_count
        return 1

class parse_insert(object):

    def __init__(self):

        pass
        
    def retrieve_fields(self, data):
        # log_format: [01/Feb/2014:00:00:00 +0000] GET /epg/schedules/pictures/13-LWxXVA%3D%3DTW/360x270 ?roomid=1&version=15 307 - 6 Dalvik/1.6.0 (Linux; U; Android 4.3; HTC_One_max Build/JSS15J) 59.125.198.178
        # timestamp
        loc1, loc2 = data.find('['), data.find(']')
        timE = data[loc1+1 : loc2-9]
        # calculating the numerical value of timezone in hours from the timestamp
        timezone = float(data[loc1 : loc2].split(" ")[1].strip("+"))/100 + (float(data[loc1 : loc2].split(" ")[1].strip("+")) % 100 )/60
        mytime = datetime.strptime(timE, '%d/%b/%Y:%H:%M')
        mytime += timedelta(hours=timezone)
        timestamp = mytime.strftime("%d.%m.%Y %H:%M")

        # request-type, request-link, req-details, user-agent, os, device and host
        data = data[loc2+2 : ]
        loc1 = data.find('/')
        req_type = data[ : loc1].strip(' ')

        if data.find('?') > -1:
            loc2 = data.find('?')
            req_link = data[loc1 : loc2].strip(' ')
            req_det = data[loc2 : loc2+data[loc2 : ].find(" ")].strip(' ')
            data = data[loc2+data[loc2 : ].find(" ") : ]
        else : # when there is no request-detail field
            req_link = data[loc1 : loc2+data[loc1 : ].find(" ")].split(' ')[0]
            req_det = '-'
            data = data[loc1+data[loc1 : ].find(" ") : ]

        if data.find('(') > -1 and data.find(')') > -1:
            user_agent = data[data.find('(') + 1 : data.find(')') - 1].strip(' ')
            temp = user_agent.split(";")
            if len(temp)>3:
                os = temp[2]
                phone_type = temp[3]
            else:
                os = '-'
                phone_type = '-'
            host = data[data.find(')') + 1 : ].strip(' ').strip('\n')
            data = data[ : data.find('(')]
        else:
            user_agent = '-'
            os = '-'
            phone_type = '-'
            host = data[data.rfind(' ') : ].strip(' ').strip('\n')
            data = data[ : data.rfind(' ')].strip(' ')

        # response-code and virtual-machine    
        data = data.strip(' ')
        try:
            loc1 = data.find(' ')
            loc2 = data.rfind(' ')
            response = data[ : loc1]
            virtualm = data[loc2+1 : ]
            data = data[loc1+1 : loc2]
        except:
            response = '-'
            virtualm = '-'

        # response-time and byte-transfer
        try:
            byte_transfer = data.split(' ')[0]
            response_time = data.split(' ')[1]
        except:
            byte_transfer = '-'
            response_time = '-'        
        if byte_transfer == '-':
            byte_transfer = 0
        if response_time == '-':
            response_time = 0
        if response == '-':
            response = 0
        return timestamp, req_type, req_link, req_det, response, byte_transfer, response_time, user_agent, host, virtualm, os, phone_type

    def insert_fields(self, batch, fields):

        new_batch = batch.insert(str(uuid.uuid1()), {"timestamp": fields[0].encode("utf-8"), "request_type": fields[1].encode("utf-8"), "request_link": fields[2].encode("utf-8"), "request_details": fields[3].encode("utf-8"), "response_code": fields[4].encode("utf-8"), "byte_transfer": int(fields[5]), "response_time": int(fields[6]), "user_agent": fields[7].encode("utf-8"), "host": fields[8].encode("utf-8"), "virtual_machine": fields[9].encode("utf-8"), "operating_system": fields[10].encode("utf-8"), "device_type": fields[11].encode("utf-8")})
        return new_batch