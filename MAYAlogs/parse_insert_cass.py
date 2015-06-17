__author__ = 'rahul'

from datetime import datetime, timedelta
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import *
from cassandra.cluster import Cluster
global t_init, file_path
t_init = datetime.now()
# initlializations
file_path = "/home/abhinav/Downloads/MayaLogs/Asia"

def create_keyspace(key_space, column_family):

    sys = SystemManager('localhost:9160')
    sys.create_keyspace(key_space, SIMPLE_STRATEGY, {'replication_factor': '1'})
    validators = {'timestamp': UTF8_TYPE, 'host': UTF8_TYPE, 'request-type': UTF8_TYPE, 'request-link': UTF8_TYPE, 'request-details': UTF8_TYPE, 'response-code': UTF8_TYPE, 'byte-transfer': UTF8_TYPE, 'response-time': UTF8_TYPE, 'user-agent': UTF8_TYPE, 'logid': UTF8_TYPE, 'virtual-machine': UTF8_TYPE}
    sys.create_column_family(key_space, column_family, super=False, comparator_type=UTF8_TYPE, key_validation_class=UTF8_TYPE, column_validation_classes=validators)
    sys.close()
    # configuring the compaction strategy of cassandra table
    session = Cluster(contact_points=['127.0.0.1'], port=9042).connect(keyspace=key_space) 
    session.execute("""ALTER TABLE "ASIA_CF" WITH compaction = {'class' :  'LeveledCompactionStrategy'} AND compression = {'sstable_compression' : 'DeflateCompressor', 'chunk_length_kb' : 64}""")
    return 1

def initialize_connection(key_space, column_family):
    
    uuid = 100000
    pool = ConnectionPool(key_space, ['localhost:9160'], timeout=60)
    col_fam = ColumnFamily(pool, column_family)
    # function call
    extract_fields(col_fam, uuid)
    print "Added data to the Cassandra database"
    return 1

def extract_fields(col_fam, num):
    
    # log_format: [01/Feb/2014:00:00:00 +0000] GET /epg/schedules/pictures/13-LWxXVA%3D%3DTW/360x270 ?roomid=1&version=15 307 - 6 Dalvik/1.6.0 (Linux; U; Android 4.3; HTC_One_max Build/JSS15J) 59.125.198.178
    row_count = 0
    text = open(file_path).readlines()
    # extracting the logdata in every line
    for data in text:
        print "Row: %s" % row_count
        # timestamp
        loc1, loc2 = data.find('['), data.find(']')
        timE = data[loc1+1 : loc2-9]
        # calculating the numerical value of timezone in hours from the timestamp
        timezone = float(data[loc1 : loc2].split(" ")[1].strip("+"))/100 + (float(data[loc1 : loc2].split(" ")[1].strip("+")) % 100 )/60
        mytime = datetime.strptime(timE, '%d/%b/%Y:%H:%M')
        # adding the timezone in the time format
        mytime += timedelta(hours=timezone)
        timestamp = mytime.strftime("%d.%m.%Y %H:%M")
    
        # request-type, request-link, req-details, userdata and host
        data = data[loc2+2 : ]
        loc1 = data.find('/')
        req_type = data[ : loc1].strip(' ')

        if data.find('?') > -1:
            loc2 = data.find('?')
            req_link = data[loc1 : loc2].strip(' ')
            req_det = data[loc2 : loc2+data[loc2 : ].find(" ")].strip(' ')
            data = data[loc2+data[loc2 : ].find(" ") : ]
        else : # when there is no request-detail field
            req_link = data[loc1 : loc2+data[loc1 : ].find(" ")].strip(' ')
            req_det = '-'
            data = data[loc1+data[loc1 : ].find(" ") : ]

        if data.find('(') > -1 and data.find(')') > -1:
            user_agent = data[data.find('(') + 1 : data.find(')') - 1].strip(' ')
            host = data[data.find(')') + 1 : ].strip(' ').strip('\n')
            data = data[ : data.find('(')]
        else:# when there is no user-data present
            user_agent = '-'
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
    
        # response-time and bytes-transfer
        try:
            byte_transfer = data.split(' ')[0]
            response_time = data.split(' ')[1]
        except:
            byte_transfer = '-'
            response_time = '-'
    
        # log-id
        try:
            # finding the logid if present in the request-link
            loc1 = req_link.find('favoritesandpopular')
            loc2 = req_link.find('recommendationsforfeeds')
            if loc1 > -1:
                logid = req_link[loc1+1+req_link[loc1 : ].rfind('/'):]
            elif loc2 > -1:
                logid = req_link[loc2+1+req_link[loc2 : ].rfind('/'):]
            else:
                logid = '-'
        except:
            logid = '-'
        # function call
        insert_fields(col_fam, num, host, timestamp, req_type, req_link, req_det, response, byte_transfer, user_agent, virtualm, response_time, logid)
        num = num + 1
        row_count = row_count + 1
    return 1

def insert_fields(col_fam, num, host, timestamp, req_type, req_link, req_det, response, byte_transfer, user_agent, virtualm, response_time, logid):
    
    t_ins = datetime.now()
    #encoding all the fields in UTF-8 format
    col_fam.insert(str(num), {"timestamp":  timestamp.encode("utf-8"), "host": host.encode("utf-8"), "request-type": req_type.encode("utf-8"),"request-link": req_link.encode("utf-8"),"request-details": req_det.encode("utf-8"),"response-code": response.encode("utf-8"),"byte-transfer": byte_transfer.encode("utf-8"),"response-time": response_time.encode("utf-8") ,"user-agent": user_agent.encode("utf-8"), "logid": logid.encode("utf-8"),"virtual-machine": virtualm.encode("utf-8")})
    print "Insertion time: %s  Time Elasped: %s" % ((datetime.now() - t_ins),(datetime.now() - t_init))
    return 1

if __name__ == '__main__':

    key_space = 'ASIA_KS'
    column_family = 'ASIA_CF'
    # creating Keyspace and ColumnFamily using pycassaShell commands
    create_keyspace(key_space, column_family)
    # function call
    initialize_connection(key_space, column_family)
    print "Total time elapsed: %s"%(datetime.now() - t_init)