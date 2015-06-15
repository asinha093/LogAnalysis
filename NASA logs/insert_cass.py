__author__ = 'abhinav'

import re
from datetime import datetime, timedelta
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import *

def initialize_connection(file_path):

    uuid = 100000
    # function call
    extract_data(file_path, uuid)
    print "Added data to the Cassandra database"
    print "Total time elapsed: %s" % (datetime.now() - t_init)
    return 1

def extract_data(file_path, num):
    row_count = 0
    text = open(file_path).readlines()
    # log_format: in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/news/sts-68-mcc-05.txt HTTP/1.0" 200 1839
    
    #extracting the logdata in every line 
    for data in text:
        print "Row: %s" % row_count
        # host_data
        try:
            # searching for host_data in the line using re identifiers 
            host = re.search('(.+?) - -', data).group(1)
        except:
            # if the re compiler doesnt find the host_data, it means its blank
            host = "-"

        # timestamp_details
        try:
            location1 = data.find('[')
            location2 = data.find('-0400')
            timestamp = data[location1 + 1:location2-4]
            mytime = datetime.strptime(timestamp, '%d/%b/%Y:%H:%M')
            # adding the timezone in the time format
            mytime += timedelta(hours=4) #timezone = -0400 hours
            date = mytime.strftime("%d.%m.%Y %H:%M")
        except:
            date = "-"
        
        # request_line
        try:
            requestline = re.search('"(.+?)"',data).group(1)
        except:
            requestline = "-"

        # status_code, byte_size
        try:
            location = data.find('" ')
            status = data[location+2:].split(" ")[0]
            bytes = (data[location:].split(" "))[2].split('\n')[0]
            # to skip the lines with errors --> no space between statuscode and bytesize data
            if " " in status:
                continue
        except:
            status = "-"
            bytes = "-" 
        # function call
        insert_data(num, host, date, requestline, status, bytes)
        num = num + 1
        row_count = row_count + 1
    return 1

def insert_data(num, host, date, requestline, status, bytes):
    t_ins = datetime.now()
    col_fam.insert(str(num), {"host": host, "timestamp": date, "requestline": requestline, "statuscode": status, "bytesize": bytes})     
    print "Insertion time: %s  Time Elasped: %s" % ((datetime.now() - t_ins),(datetime.now() - t_init))
    return 1

if __name__ == '__main__':

    global col_fam, t_init
    t_init = datetime.now()
    file_path = "/home/abhinav/Downloads/NASA_Aug95"
    
    # creating Keyspace and ColumnFamily using pycassaShell commands
    sys = SystemManager('localhost:9160')
    sys.create_keyspace('main_keyspace', SIMPLE_STRATEGY, {'replication_factor': '1'})
    validators = {'host': UTF8_TYPE,'timestamp': UTF8_TYPE, 'requestline': UTF8_TYPE, 'statuscode': UTF8_TYPE, 'bytesize': UTF8_TYPE}
    sys.create_column_family('main_keyspace', 'main_column', super=False, comparator_type=UTF8_TYPE, key_validation_class=UTF8_TYPE, column_validation_classes=validators)
    sys.close()

    # connecting to the keyspace and columnfamily
    cass_client = ConnectionPool('main_keyspace', ['localhost:9160'])
    col_fam = ColumnFamily(cass_client, 'main_column')
    # function call
    initialize_connection(file_path) 