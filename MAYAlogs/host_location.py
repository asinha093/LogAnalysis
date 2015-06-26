__author__ = 'rahul'
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
from datetime import datetime
import multiprocessing
import requests
import Queue
import threading
import time
cluster = Cluster()
session = cluster.connect('main')
session.set_keyspace("main")
session.default_timeout = 100
# called by each thread
def update_cassandra(host,country ,uid, list):
    query = "UPDATE ipdata SET country=%s WHERE uid= %s"
    session.execute(query, parameters=[country,uid])
    return list


def insert_cassandra(uid, country, country_count):
    qwe = session.execute("""INSERT INTO  main_counts (uid, country, count_country ) VALUES ( %s, %s, %s)""",(uid, country, country_count))
    return 1




def get_url(q, url,uid):
    q.put(location(url,uid))


def count_country(country,list):
    if country in list.keys():
            list[country] += 1
    else:
            list[country] = 1
    return list



def location(keys,uid,list):
    #print keys
    try:
        r = requests.get('http://www.telize.com/geoip/'+keys)
        content = r.json()
        country = content['country_code']
    except:
        country = '-'


    #print "location done"
    print country
    t = count_country(country,list)
    return update_cassandra(keys,country,uid,list)




def initilize_conn():
    query = "SELECT host,uid FROM ipdata"  # users contains 100 rows
    statement = SimpleStatement(query, fetch_size=400)
    t = session.execute(statement)
    c = 0
    q = Queue.Queue()
    t12 = datetime.now()
    print t12
    my_threads = []
    list = {}
    for u in t:
        awe = str(u[0]).strip()
        print u[1]
        t22 = datetime.now()
        if awe.find(',') == -1:
            list = location(awe,u[1],list)
            #data = threading.Thread(target=get_url, args = (q,awe, u[1]))
        else:
            list =location(awe.split(',')[0],u[1],list)
        if c>1000:
            break

        c += 1
        print c, datetime.now() - t12, datetime.now() - t22
    insert_cassandra(100000,list.keys(),list.values())
    print datetime.now() -t12


    print 'done'
if __name__ == '__main__':
    initilize_conn()

