__author__ = 'abhinav'
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

from pyspark import SparkConf
from pyspark_cassandra.context import *
from datetime import datetime

# configuring spark with cassandra keyspace and columnfamily
global sc
conf = SparkConf().set("spark.cassandra.connection.host", '127.0.0.1').set("spark.cassandra.connection.native.port",'9042')
sc = CassandraSparkContext(conf=conf)

if __name__ == '__main__':
    # creating class instances and calling them in their respective files 
    print "parse_insert_cass"
    t_init = datetime.now()
    init = file1.initialize('test', 'parsed_data', 'Asia', '127.0.0.1', '9160')
    init.get_file()
    print "Total time elapsed: %s\n"%(datetime.now() - t_init)
    print "timecount_insert"
    init = file2.initialize('test', 'parsed_data', 'time_counts', sc , '127.0.0.1', '9160', 9042, 'main_count')
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)  
    print "forecast_count"
    init = file3.initialize('test', 'parsed_data', 'traffic', sc , '127.0.0.1', '9160', 9042)
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "forecast_insert" 
    init = file4.initialize('test', 'traffic', 'main_count', '127.0.0.1', 9042)
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)  
    print "main_counts"    
    init = file5.initialize('test', 'parsed_data', 'main_count', sc, '127.0.0.1', 9042)
    init.initilize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "hostcount_insert"
    init = file6.initialize('test', 'parsed_data', 'ipdata', sc, '127.0.0.1', '9160', 9042)
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "logid_insert"
    init = file7.initialize('test', 'parsed_data', 'main_count', sc, '127.0.0.1', '9160', 9042)
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
    print "timeid_insert"
    init = file8.initialize('test', 'time_counts', 'main_count', sc, '127.0.0.1', '9160', 9042)
    init.initialize_connection()
    print "Total Time Elapsed: %s" % (datetime.now() - t_init)
