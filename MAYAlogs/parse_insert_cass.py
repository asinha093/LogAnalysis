__author__ = 'rahul'
from datetime import datetime
from cassandra.cluster import Cluster
from pymongo import MongoClient
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
import uuid
cluster = Cluster()
session = cluster.connect('main')
cass_client = ConnectionPool('main',['localhost:9160'])
col_fam = ColumnFamily(cass_client,'parsed_data_cli')
def insert_mongo(temp):
    data = {"timestamp":temp[0],"reqtype":temp[1],"link":temp[2],"reqdetails":temp[3],"response":temp[4],"byte_transfer":temp[5],"response_time":temp[9] ,"user_data":temp[6],"host":temp[7],"vm":temp[9]}
    add1 = dest.insert(data)
    print datetime.now().time()
    return add1

def insert_cassandra(timestamp, req_type, req_link, req_det, response, byte_transfer, user_data, host, vm, response_time, os, phone_type):
    temp = 0
    id1 = uuid.uuid1()
    qwe = col_fam.insert(str(id1), {"timestamp":  timestamp.encode("utf-8"), "req_type": req_type.encode("utf-8"),"req_link": req_link.encode("utf-8"),"req_details": req_det.encode("utf-8"),"response": int(response),"byte_transfer": int(byte_transfer),"response_time": int(response_time), "user_agent": user_data.encode("utf-8"),"host": host.encode("utf-8"),"virt_mach": vm.encode("utf-8"),"os": os.encode("utf-8"),"device_type": phone_type.encode("utf-8")})
    #qwe = session.execute("""INSERT INTO parsed_data (uid, timestamp, req_type, req_link ,req_details ,response, byte_transfer, response_time, user_agent,  host, virt_mach, os, phone_type,timeid)  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)""", (id1, timestamp, req_type, req_link, req_det, response, byte_transfer, response_time,user_data, host, vm, os, phone_type, temp ))
    #encoding all the fields in UTF-8 format
    return qwe




def extract_fields(data): #Extracting fields based on the format using find function
    a, b = data.find('[') , data.find(']')
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
    #print req_link, req_det, req_type
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
    if byte_transfer=='-':
        byte_transfer = 0
    if response_time == '-':
        response_time = 0
    if response == '-':
        response = 0
    return insert_cassandra(timestamp, req_type, req_link, req_det, response, byte_transfer, user_data, host, vm, response_time, os, phone_type)

def source_read(dbase,src,dst):
    global client, cass_client, col_fam, uuid, dest
    cass_client = ConnectionPool(dbase,['localhost:9160'])
    sour = ColumnFamily(cass_client,src)
    i =1
    for x in range(0,9999999):
        t1 = datetime.now()
        holder = sour.get(str(x))
        qwer = datetime.now()
        t = extract_fields(holder['content'])
        print datetime.now() - qwer, x
        x += 1


if __name__ == '__main__':
    source_read('main','random1','parsed_data')




