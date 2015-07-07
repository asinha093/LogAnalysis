'''
This file is the master pythn file. It imports all the individual python scripts and calls them through their classes to build the project!
'''
# imorting python scripts as modules 
import parse_insert_cass as file1
import timecount_insert as file2
import forecast_count as file3
import forecast_insert as file4
import main_counts as file5
import hostcount_insert as file6
import logid_insert as file7
import timeid_insert as file8
import ConfigParser
from pyspark import SparkConf
from pyspark_cassandra.context import *
from datetime import datetime

# configuring spark with cassandra keyspace and columnfamily

if __name__ == '__main__':
    cfgfile = open("configuration.ini",'r')
    Config = ConfigParser.SafeConfigParser()
    Config.read("configuration.ini")
    options = Config.options("user_settings")
    settings = {}
    for option in options:
        settings[option] = Config.get("user_settings", option)
        if settings[option] == '-' :
            settings[option] = Config.get("default_settings", option)
    # creating class instances and calling them in their respective files
    print settings
    global sc
    conf = SparkConf().set("spark.cassandra.connection.host", settings['spark_cluster']).set("spark.cassandra.connection.native.port", settings['spark_port'])
    sc = CassandraSparkContext(conf=conf)

    print "parse_insert_cass"
    t_init = datetime.now()
    init = file1.initialize(settings['cluster_name'], 'parsed_data', settings['file_location'], settings['cass_cluster'], settings['thrift_port'])
    init.get_file()
    print "Total time elapsed: %s\n"%(datetime.now() - t_init)
    print "timecount_insert"
    init = file2.initialize(settings['cluster_name'], 'parsed_data', 'time_counts', sc , settings['cass_cluster'], settings['thrift_port'], settings['cql_port'], 'main_count')
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "forecast_count"
    init = file3.initialize(settings['cluster_name'], 'parsed_data', 'traffic', sc , settings['cass_cluster'], settings['thrift_port'], settings['cql_port'])
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "forecast_insert"
    init = file4.initialize(settings['cluster_name'], 'traffic', 'main_count', settings['cass_cluster'], settings['cql_port'])
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "main_counts"
    init = file5.initialize(settings['cluster_name'], 'parsed_data', 'main_count', sc, settings['cass_cluster'], settings['cql_port'])
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "hostcount_insert"
    init = file6.initialize(settings['cluster_name'], 'parsed_data', 'ipdata', sc, settings['cass_cluster'], settings['thrift_port'], settings['cql_port'])
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "logid_insert"
    init = file7.initialize(settings['cluster_name'], 'parsed_data', 'main_count', sc, settings['cass_cluster'], settings['thrift_port'], settings['cql_port'])
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "timeid_insert"
    init = file8.initialize(settings['cluster_name'], 'time_counts', 'main_count', sc, settings['cass_cluster'], settings['thrift_port'], settings['cql_port'])
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "Please execute the dashboard python script to start running the UI"
