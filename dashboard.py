from flask import Flask, render_template, jsonify, request
from flask_cassandra import CassandraCluster
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.index import *
from cassandra.cluster import Cluster
import ConfigParser
from cassandra.query import SimpleStatement
from datetime import datetime
global cassandra, settings
cfgfile = open("configuration.ini",'r')
Config = ConfigParser.SafeConfigParser()
Config.read("configuration.ini")
options = Config.options("user_settings")
settings = {}
for option in options:
    settings[option] = Config.get("user_settings", option)
    if settings[option] == '-' :
        settings[option] = Config.get("default_settings", option)
cassandra = CassandraCluster()
app = Flask(__name__)
app.config['CASSANDRA_NODES'] = [settings['cass_cluster']]  # can be a string or list of nodes
@app.route('/')
def homepage(chartID = 'chart_ID', chart_type = 'line', chart_height = 600):
    t11 = datetime.now()
    session = cassandra.connect()
    session.set_keyspace(settings['cluster_name'])
    session.default_timeout = 100
    try :
        key1 = int(request.args.get('key'))
    except:
        key1=10
    print key1
    if key1 == None:
        key1=10
    time, response_time, total_visits, unique_visits = [], [], [], []
    statement = SimpleStatement("SELECT * FROM time_counts", fetch_size=100)
    timecount = session.execute(statement)
    for user_row in timecount:
        time.append(user_row[12].encode('utf-8'))
        response_time.append(int(user_row[11]))
        total_visits.append(int(user_row[13]))
        unique_visits.append(int(user_row[14])) 
    query2 = "SELECT device_count,device_list,os_list,os_count FROM main_count"
    statement2 = SimpleStatement(query2, fetch_size=100)
    reqtype, reqtype_count, device_list, device_count, vm_list, vm_count = [], [], [], [], [], []
    temp_data = session.execute(query2)
    os_list, os_count = [], []
    for user_row in temp_data:
        k = 0
        device_list = [[]for i in range(0,len(user_row[0]))]
        for t in range(0,len(user_row[0])):
            device_list[t].append(user_row[1][t].encode('utf-8').strip(" "))
            device_list[t].append(user_row[0][t])
            k += 1

        os_data = [[]for i in range(0,len(user_row[2]))]
        for t in range(0,len(user_row[2])):
            if user_row[2][t] == '-':
                os_data[t].append("not-set")
            else:
                os_data[t].append(user_row[2][t].strip(" "))
            os_data[t].append(user_row[3][t])
    query3 = "SELECT req_list,req_count,vm_list,vm_count FROM main_count"
    statement3 = SimpleStatement(query3, fetch_size=150)
    temp_data = session.execute(statement3)
    for user_row in temp_data:
        for t in range(0,len(user_row[0])):
            if user_row[1][t] > 10:
                reqtype.append(user_row[0][t].encode('ascii'))
                reqtype_count.append(user_row[1][t])
            else: continue

        for t in range(0,len(user_row[3])):
            if user_row[3][t] > 10:
                vm_list.append(user_row[2][t].encode('utf-8'))
                vm_count.append(user_row[3][t])
            else: continue
    query4 = "SELECT time_error400, link_error400,time_error500, link_error500 FROM main_count"
    statement4 = SimpleStatement(query4, fetch_size=150)
    temp_data = session.execute(statement4)
    for user_row in temp_data:
        len1 = len(user_row[0]) + len(user_row[2])
        temp1 = len(user_row[0])
        error_data = [[]for i in range(0,len1)]
        for i in range(0,len(user_row[0])):
            error_data[i].append(user_row[0][i])
            error_data[i].append("400")
            error_data[i].append(user_row[1][i])
        for j in range(0,len(user_row[2])):
            error_data[temp1+j].append(user_row[2][j])
            error_data[temp1+j].append("500")
            error_data[temp1+j].append(user_row[3][j])
    query5 = "SELECT country, country_count FROM main_count"
    statement5 = SimpleStatement(query5, fetch_size=150)
    temp_data = session.execute(statement5)
    country, country_count = [], []
    for user_row in temp_data:
        if user_row[0]:
            country.append(user_row[0])
            country_count.append(user_row[1])






    return render_template('homepage.html', chartID=chartID, timestamp=time, resp=response_time, tvis=total_visits, uvis=unique_visits, req = reqtype, reqc = reqtype_count, vm = vm_list, vm_c= vm_count, os_temp = os_data, phn = device_list, phn_c= device_count, error_data = error_data,country = country, country_count = country_count)

@app.route('/data')
def datatable():
    session = cassandra.connect()
    session.set_keyspace(settings['cluster_name'])
    cass_client = ConnectionPool(settings['cluster_name'],[settings['cass_cluster']+':'+settings['thrift_port']], timeout=60)
    col_fam = ColumnFamily(cass_client,'parsed_data')
    session.default_timeout = 100
    key1 = request.args.get('key')
    if key1 == None:
        key1=100
    key1= int(key1)
    query = "SELECT last_timeid FROM main_count"  # users contains 100 rows
    statement = session.execute(query)
    for x in statement:
        end_key = x[0]
    log_data = [[]for i in range(0,key1)]
    log_data_header,k = [], 0
    temp = end_key-key1
    for i in range(temp,end_key):
        expr2 = create_index_expression('logid',i)
        lause = create_index_clause([ expr2], count=1)
        test = list(col_fam.get_indexed_slices(lause, columns= ["timestamp", "host", "byte_transfer", "request_link", "request_details", "device_type", "operating_system", "request_type", "response_code", "response_time"]))[0][1]
        for m in test.values():
            log_data[k].append(m)
        k += 1
        if k == 1:
            for n in test.keys():
                log_data_header.append(n)
    return render_template('datatable.html', data= log_data, data_header = log_data_header,index = key1)

@app.route('/forecast')
def forecast(chartID = 'chart_ID', chart_type = 'spline', chart_height = 150):
    session = cassandra.connect()
    session.set_keyspace(settings['cluster_name'])
    session.default_timeout = 100
    key1 = request.args.get('key')

    if key1 == None:
        key1=10
    key1 = int(key1)
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    query1 = "SELECT time_sorted,bytes_original,bytes_predicted,gets_original,gets_predicted FROM main_count"  # users contains 100 rows
    statement1 = SimpleStatement(query1, fetch_size=100)
    i = 0
    t11 = datetime.now()
    data1 = session.execute(statement1)
    for user_row in data1:
        time = (user_row[0])
        bytes_original = user_row[1]
        bytes_predicted = user_row[2]
        gets_original = user_row[3]
        gets_predicted = user_row[4]
    query2 = "SELECT posts_original,posts_predicted,requests_original,requests_predicted,visits_original,visits_predicted FROM main_count"  # users contains 100 rows
    statement2 = SimpleStatement(query2, fetch_size=100)
    data1 = session.execute(statement2)
    for user_row in data1:
        posts_original = user_row[0]
        posts_predicted = user_row[1]
        requests_original = user_row[2]
        requests_predicted = user_row[3]
        visits_original = user_row[4]
        visits_predicted = user_row[5]
    return render_template('prediction.html', chartID=chartID, timestamp=time, val1=bytes_original, val2=bytes_predicted, val3=gets_original, val4=gets_predicted, val5= posts_original, val6=posts_predicted, val7=requests_original, val8=requests_predicted, val9=visits_original, val10=visits_predicted)

@app.route('/getinfo')
def datale():
    session = cassandra.connect()
    session.set_keyspace(settings['cluster_name'])
    cass_client = ConnectionPool(settings['cluster_name'],[settings['cass_cluster']+':'+settings['thrift_port']], timeout=60)
    col_fam = ColumnFamily(cass_client,'parsed_data')
    session.default_timeout = 100
    key1 = request.args.get('key')
    expr2 = create_index_expression('timestamp',key1)
    lause = create_index_clause([ expr2], count=99999)

    test = list(col_fam.get_indexed_slices(lause, columns= ["timestamp", "host", "byte_transfer", "request_link", "request_details", "device_type", "operating_system", "request_type", "response_code", "response_time"]))
    data, data_header = [[]for i in range(0,len(test))], []
    k = 0
    for k in range(0,len(test)):
        for m in test[k][1].values():
            data[k].append(m)
            if k == 1:
                for n in test[k][1].keys():
                    data_header.append(n)

    #return json.dumps(test)
    return render_template('datatable.html', data= data, data_header = data_header,index = len(test))

if __name__ == "__main__":

    # creating class instances and calling them in their respective files
    app.run(debug = True, passthrough_errors=True, threaded=True)