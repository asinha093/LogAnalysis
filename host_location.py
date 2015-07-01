__author__ = 'rahul'
'''
This file retrieves the location details of all the hosts in the table: ipdata.
The file extracts the country_codes associated with their respective ip addresses and updates them to the existing cassandra table: ipdata.
This file also prepares 2 lists: 1) a list containg all country_codes and 2) a list containing their respective counts, and inserts them
into the existing table: main_counts which will be used for building the UI !
'''
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
import requests
import uuid

class initialize_insert(object):

    def __init__(self, keyspace, source, table, host, port):
        # initializing the class variables
        self.keyspace = keyspace
        self.source = source
        self.table = table
        self.host = host
        self.port = port

    def insert_cassandra(self, session, source, country, country_count):

        query = "SELECT id FROM "+source
        uid = session.execute(query)[0][0]
        # retrieving the id from the source table and updating the country_count (list) and country (list) columns
        session.execute("UPDATE "+source+" SET country=%s, country_count=%s WHERE id=%s)", parameters=[country, country_count, uid])
        return 1

    def initialize_connection(self):

        session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        query = "SELECT host, id FROM "+self.table
        statement = SimpleStatement(query)
        getdata = session.execute(statement)
        hosts, id_val, count = [], [], 0
        for data in getdata:
            value = str(data[0]).strip()
            id_val.append(data[1])
            if value.find(',') == -1:
                hosts.append(value)
            else:
                hosts.append(value.split(",")[0])
            count += 1
        print count
        # create instance for the class retrieve_location
        process = retrieve_location()
        # function calls
        count_list = process.get_location(session, self.table, hosts, id_val)
        self.insert_cassandra(session, self.source, count_list.keys(), count_list.values())
        return 1

class retrieve_location(object):

    def __init__(self):

        pass

    def get_location(self, session, table, hosts, uid):

        country = []
        i = 1
        for ip in hosts:
            try: # retrieving the location details from the web server
                rqst = requests.get('http://www.telize.com/geoip/'+ip)
                content = rqst.json()
                print content['country_code'], i
                country.append(content['country_code'])
            except:
                country.append('-')
            i+=1
        # function calls
        temp = self.count_country(country)
        print "count done dict"
        self.update_cassandra(session, table, country, uid)
        return temp 

    def count_country(self, country_list):

        tempdict = {}
        # creating a dictionary containg key as country_code and value as its count
        for country in country_list:
            if country in tempdict.keys():
                    tempdict[country] += 1
            else:
                    tempdict[country] = 1
        return tempdict

    def update_cassandra(self, session, table, country, uid):

        prepared_stmt = session.prepare ("UPDATE "+table+" SET country = ? WHERE (id = ?)")
        # inserting the country_code to the table containg host details
        for index in range(0, len(uid)):
            bound_stmt = prepared_stmt.bind([country[index], uid[index]])
            stmt = session.execute(bound_stmt)
            print country[index], uid[index]
        return 1