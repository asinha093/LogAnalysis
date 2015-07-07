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
from geoip import geolite2
from pyspark import SparkConf
from pyspark_cassandra.context import *
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

        query = "SELECT id FROM main_count"
        print query
        session2 = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        session2.default_timeout = 100
        data = session2.execute(query)
        for row in data:
            uid = row[0]
        # retrieving the id from the source table and updating the country_count (list) and country (list) columns
        print len(country), len(country_count)
        session.execute("UPDATE "+source+" SET country=%s, country_count=%s WHERE id=%s", parameters=[country, country_count, uid])
        return 1

    def initialize_connection(self):

        session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
        session.default_timeout = 100
        query = "SELECT host, id FROM "+self.table
        statement = SimpleStatement(query)
        getdata = session.execute(statement)
        hosts, id_val, count = [], [], 0
        count_list = {}
        for data in getdata:
            value = str(data[0]).strip()
            id_val.append(data[1])
            if value.find(',') == -1:
                hosts.append(value)
            else:
                hosts.append(value.split(",")[0])
            count += 1
        print count
        #create instance for the class retrieve_location
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
            temp = str(ip).strip()
            if temp.find(',') == -1:
                try:
                    country.append(geolite2.lookup(temp).country)
                    i += 1
                except:
                    country.append('-')
            else:
                try:
                    country.append(geolite2.lookup(temp.split(',')[0]))
                    i += 1
                except:
                    country.append('-')
        # function calls
        temp = self.count_country(country)
        print "count done dict"
        self.update_cassandra(session, table, country, uid)
        return temp

    def count_country(self, country_list):

        tempdict = {}
        # creating a dictionary containg key as country_code and value as its count
        for country in country_list:
            if country == None:
                country = '-'
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
if __name__ == '__main__':

    t_init = datetime.now()
    conf = SparkConf().set("spark.cassandra.connection.host", '127.0.0.1').set("spark.cassandra.connection.native.port",'9042')
    sc = CassandraSparkContext(conf=conf)
    getdata = initialize_insert('test', 'main_count', 'ipdata', '127.0.0.1', 9042)
    getdata.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)