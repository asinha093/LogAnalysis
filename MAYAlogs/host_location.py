__author__ = 'rahul'
from cassandra.query import SimpleStatement
from cassandra.cluster import Cluster
import requests
import uuid
from datetime import datetime



def update_cassandra(conn,db,country,uid):
    prepared_stmt = conn.prepare ("UPDATE "+db+" SET country = ? WHERE (uid = ?)")
    for i in range(0,len(uid)):
        bound_stmt = prepared_stmt.bind([country[i],uid[i]])
        stmt = conn.execute(bound_stmt)
        print country[i],uid[i]
    return 1


def insert_cassandra(conn, maindb,country, country_count):
    id = uuid.uuid1()
    qwe = conn.execute("""INSERT INTO  """+maindb+""" (uid, country, count_country ) VALUES ( %s, %s, %s)""",(id, country, country_count))
    return 1

def count_country(country_list):
    temp={}
    for country in country_list:
        if country in temp.keys():
                temp[country] += 1
        else:
                temp[country] = 1
    print temp
    return temp



def location(session,db,host,uid):
    country = []
    for ip in host:
        try:
            r = requests.get('http://www.telize.com/geoip/'+ip)
            content = r.json()
            print content
            country.append(content['country_code'])
        except:
            country.append('-')

    t = count_country(country)
    update_cassandra(session,db,country,uid)
    return t




def initilize_conn(session,maindb,db):
    query = "SELECT host,uid FROM "+db
    statement = SimpleStatement(query, fetch_size=400)
    t = session.execute(statement)
    list,te1 = {}, datetime.now()
    hosts,id_val,count = [], [], 0
    for u in t:
        awe = str(u[0]).strip()
        id_val.append(u[1])
        if awe.find(',') == -1:
            hosts.append(awe)

        else:
            hosts.append(awe.split(",")[0])
        count += 1
        if count>10:
            break
    print count, datetime.now()-te1
    count_list = location(session,db,hosts,id_val)
    insert_cassandra(session, maindb, count_list.keys(), count_list.values())


    print 'done'
if __name__ == '__main__':
    cluster = Cluster()
    session = cluster.connect('main')
    session.default_timeout = 100
    initilize_conn(session, "main_counts","ipdata")

