__author__ = 'rahul'
from datetime import datetime
from pymongo import MongoClient
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily

def insert_mongo(temp):
    data = {"timestamp":temp[0],"reqtype":temp[1],"link":temp[2],"reqdetails":temp[3],"response":temp[4],"byte_transfer":temp[5],"response_time":temp[9] ,"user_data":temp[6],"host":temp[7],"vm":temp[9]}
    add1 = dest.insert(data)
    print datetime.now().time()
    return add1

def insert_cassandra(timestamp, req_type, req_link, req_det, response, byte_transfer, user_data, host, vm, response_time,num, logid):
    qwe = col_fam.insert(str(num), {"timestamp":  timestamp.encode("utf-8"), "reqtype": req_type.encode("utf-8"),"reqtlink": req_link.encode("utf-8"),"reqdetails": req_det.encode("utf-8"),"response": response.encode("utf-8"),"byte_transfer": byte_transfer.encode("utf-8"),"response_time": response_time.encode("utf-8") ,"user_data": user_data.encode("utf-8"),"host": host.encode("utf-8"),"logid": logid.encode("utf-8"),"virt_mach": vm.encode("utf-8")})
    #encoding all the fields in UTF-8 format
    return qwe




def extract_fields(data,num): #Extracting fields based on the format using find function
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
        host = data[data.find(')')+1:].strip(' ').strip('\n')
        data = data [:data.find('(')]
    else:
        user_data = '-'
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
        response = '-'
        vm = '-'
    try:
        byte_transfer = data.split(' ')[0]
        response_time = data.split(' ')[1]
    except:
        byte_transfer = '-'
        response_time = '-'
    try:
        a = req_link.find('favoritesandpopular')
        b = req_link.find('recommendationsforfeeds')
        if a > -1:
            logid = req_link[a+1+req_link[a:].rfind('/'):]
        elif b > -1:
            logid = req_link[b+1+req_link[b:].rfind('/'):]
        else:
            logid = '-'
    except:
        logid = '-'
    #log = [timestamp, req_type, req_link, req_det, response, byte_transfer, user_data, host, vm, response_time]
    return insert_cassandra(timestamp, req_type, req_link, req_det, response, byte_transfer, user_data, host, vm, response_time, num, logid)
    #return 1

def source_read(dbase,src,dst):
    global client, cass_client, col_fam, uuid, dest
    client = MongoClient(max_pool_size = 99999)
    uuid = 100000
    db = client[dbase]
    sour = db[src]
    dest = db[dst]
    holder = sour.find()
    cass_client = ConnectionPool('main',['localhost:9160'])
    col_fam = ColumnFamily(cass_client,'random3')
    for k in holder:
        t =extract_fields(k['content'],uuid)
        uuid = uuid + 1

if __name__ == '__main__':
    source_read('trial','raw_data1','parsed_data')




