__author__ = 'rahul'
from datetime import datetime
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.system_manager import *
from pycassa.batch import *
import uuid



def insert_cassandra(batch,data):
    id1 = uuid.uuid1()
    new_batch = batch.insert(str(id1), {"timestamp":  data[0].encode("utf-8"), "req_type": data[1].encode("utf-8"),"req_link": data[2].encode("utf-8"),"req_details": data[3].encode("utf-8"),"response": int(data[4]),"byte_transfer": int(data[5]),"response_time": int(data[9]), "user_agent": data[6].encode("utf-8"),"host": data[7].encode("utf-8"),"virt_mach": data[8].encode("utf-8"),"os": data[10].encode("utf-8"),"device_type": data[11].encode("utf-8")})
    return new_batch

def create_keyspace(cluster, key_space, column_fam):

    sys = SystemManager(cluster)
    sys.create_keyspace(key_space, SIMPLE_STRATEGY, {'replication_factor': '1'})
    validators = {'timestamp': UTF8_TYPE,'req_type': UTF8_TYPE, 'req_link': UTF8_TYPE, 'req_deatils': UTF8_TYPE, 'response': INT_TYPE, 'byte_transfer': INT_TYPE, 'response_time': INT_TYPE, 'user_agent': UTF8_TYPE, 'virt_mach': UTF8_TYPE, 'os': UTF8_TYPE, 'device_type': UTF8_TYPE, 'host': UTF8_TYPE, 'log_id': INT_TYPE}
    sys.create_column_family(key_space, column_fam, super=False, comparator_type=UTF8_TYPE, key_validation_class=UTF8_TYPE, column_validation_classes=validators)
    sys.create_index(key_space,column_fam,"host",UTF8_TYPE,index_type=KEYS_INDEX)
    sys.create_index(key_space,column_fam,"timestamp",UTF8_TYPE,index_type=KEYS_INDEX)
    sys.create_index(key_space,column_fam,"log_id",INT_TYPE,index_type=KEYS_INDEX)
    sys.close()
    return 1






def extract_fields(data,conn): #Extracting fields based on the format using find function
    a, b = data.find('['), data.find(']')
    timestamp = data[a+1:b-1].split(' ')[0]
    date = timestamp[1:timestamp.find(':')]
    time = timestamp[timestamp.find(':')+1:timestamp.find(':')+9]
    date = datetime.strptime(date, '%d/%b/%Y').strftime('%d.%m.%Y')
    timestamp = date + ' ' + time[:5]
    data= data[b+1:]
    a = data.find('/')
    req_type = data[:a].strip(' ')
    if data.find('?') >-1:
        b = data.find('?')
        req_link = data[a:b].strip(' ')
        req_det = data[b:b+data[b:].find(" ")].strip(' ')
        data = data[b+data[b:].find(" "):]
    else :
        req_link = data[a:a+data[a:].find(" ")].strip(' ')
        req_det = '-'
        data = data[a+data[a:].find(" "):]
    if data.find('(') >-1 and data.find(')') > -1:
        user_data = data[data.find('(')+1:data.find(')')-1].strip(' ')
        temp = user_data.split(";")
        if len(temp)>3:
            os = temp[2]
            phone_type = temp[3]
        else:
            os = '-'
            phone_type = '-'
        host = data[data.find(')')+1:].strip(' ').strip('\n')
        data = data [:data.find('(')]
    else:
        user_data = '-'
        os = '-'
        phone_type = '-'
        host = data[data.rfind(' '):].strip(' ').strip('\n')
        data = data[:data.rfind(' ')].strip(' ')

    data = data.strip(' ')
    try:
        a = data.find(' ')
        b = data.rfind(' ')
        response = data[:a]
        vm = data[b+1:]
        data = data[a+1:b]
    except:
        response = 0
        vm = '-'
    try:
        byte_transfer = data.split(' ')[0]
        response_time = data.split(' ')[1]
    except:
        byte_transfer = 0
        response_time = 0
    if byte_transfer == '-':
        byte_transfer = 0
    if response_time == '-':
        response_time = 0
    if response == '-':
        response = 0
    return insert_cassandra(timestamp, req_type, req_link, req_det, response, byte_transfer, user_data, host, vm, response_time, os, phone_type, conn)

def source_read(cluster,dbase,src,dst):
    create_keyspace(cluster,dbase,dst)
    cass_client = ConnectionPool(dbase,[cluster])
    source = ColumnFamily(cass_client,src)
    dest_conn = ColumnFamily(cass_client,dst)
    batch = dest_conn.batch(queue_size=1000)
    for x in range(0,9999999):
        t1 = datetime.now()
        holder = source.get(str(x))
        qwer = datetime.now()
        fields = extract_fields(holder['content'])
        batch = insert_cassandra(batch, fields)
        if x % 1000 == 0:
            # inserting in batches of 1000
            batch.send()
            print "batch sent"
            # creating a fresh batch
            batch = dest_conn.batch(queue_size=1000)
        elif x == 99999999:
            # inserting the final batch
            batch.send()
        print datetime.now() - qwer, x
        x += 1



if __name__ == '__main__':

    source_read('localhost:9160','main','random1','parsed_data')




