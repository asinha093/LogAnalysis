from flask import Flask, render_template, jsonify, request
from flask_cassandra import CassandraCluster
from pycassa.pool import ConnectionPool
from pycassa.columnfamily import ColumnFamily
from pycassa.index import *
import pycassa
from cassandra.query import SimpleStatement
from datetime import datetime
import json
cassandra = CassandraCluster()
app = Flask(__name__)
app.config['CASSANDRA_NODES'] = ['127.0.0.1']  # can be a string or list of nodes

@app.route('/forecast')
def forecast(chartID = 'chart_ID', chart_type = 'spline', chart_height = 150):
    session = cassandra.connect()
    session.set_keyspace("ASIA_KS")
    session.default_timeout = 100
    try :
        key1 = int(request.args.get('key'))
    except:
        key1=10
    if key1 == None:
        key1=10
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    query1 = "SELECT * FROM ASIA_FORECAST"  # users contains 100 rows
    statement1 = SimpleStatement(query1, fetch_size=400)
    i = 0
    t11 = datetime.now()
    data1 = session.execute(statement1)
    for user_row in data1:
        time = (user_row[9])
        bytes_original = user_row[1]
        bytes_predicted = user_row[2]
        gets_original = user_row[3]
        gets_predicted = user_row[4]
        posts_original = user_row[5]
        posts_predicted = user_row[6]
        requests_original = user_row[7]
        requests_predicted = user_row[8]
        visits_original = user_row[10]
        visits_predicted = user_row[11]
        i += 1
        if i>key1:
            break

    time_taken = datetime.now() - t11
    return render_template('forecast.html', chartID=chartID, timestamp=time, val1=bytes_original, val2=bytes_predicted, val3=gets_original, val4=gets_predicted, val5= posts_original, val6=posts_predicted, val7=requests_original, val8=requests_predicted, val9=visits_original, val10=visits_predicted, rand1=len(time), rand2=len(bytes_predicted))

@app.route('/')
def index(chartID = 'chart_ID', chart_type = 'line', chart_height = 150):
    session = cassandra.connect()
    session.set_keyspace("ASIA_KS")
    session.default_timeout = 100
    try :
        key1 = int(request.args.get('key'))
    except:
        key1=10
    if key1 == None:
        key1=10
    chart = {"renderTo": chartID, "type": chart_type, "height": chart_height,}
    query1 = "SELECT * FROM ASIA_FORECAST"  # users contains 100 rows
    statement1 = SimpleStatement(query1, fetch_size=400)
    i = 0
    t11 = datetime.now()
    data1 = session.execute(statement1)
    for user_row in data1:
        time = (user_row[7])
        bytes_original = user_row[1]
        bytes_predicted = user_row[2]
        gets_original = user_row[3]
        gets_predicted = user_row[4]
        i += 1
        if i>key1:
            break

    time_taken = datetime.now() - t11
    return render_template('dashboard.html', chartID=chartID, timestamp=time, val1=bytes_original, val2=bytes_predicted, val3=gets_original, val4=gets_predicted, rand1=len(time), rand2=len(bytes_predicted))

if __name__ == "__main__":
    app.run(debug = True, passthrough_errors=True)