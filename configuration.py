from flask import Flask, render_template, jsonify, request, abort, url_for, Response, stream_with_context
import ConfigParser


app = Flask(__name__)

@app.route('/main/')
def index():
    return render_template('config_main.html' )


@app.route('/config', methods=['POST'])
def method():
    data,qwe = {}, []
    data['file_location'] = request.form['file_location']
    if request.form['cass_cluster']:
        data['cass_cluster'] = request.form['cass_cluster'].split(':')[0]
        data['thrift_port'] = request.form['cass_cluster'].split(':')[1]
    else:
        data['cass_cluster'] = '-'
        data['thrift_port'] = '-'

    if request.form['spark_config']:
        data['spark_cluster'] = request.form['spark_config'].split(':')[0]
        data['spark_port'] = request.form['spark_config'].split(':')[1]
    else:
        data['spark_cluster'] = '-'
        data['spark_port'] = '-'

    data['cluster_name'] = request.form['cluster_name']
    data['cql_port'] = request.form['cql_port']

    default = {'cass_cluster':'127.0.0.1', 'thrift_port':'9160', 'cluster_name':'test', 'spark_cluster': '127.0.0.1' , 'spark_port':'9042','cql_port':'9042'}
    cfgfile = open("configuration.ini",'w')
    Config = ConfigParser.SafeConfigParser()
    Config.read("configuration.ini")
# add the settings to the structure of the file, and lets write it out...
    temp = "user_settings"
    Config.add_section(temp)
    for rule in data.keys():
        Config.set("user_settings",rule, data[rule])
    Config.add_section("default_settings")
    for rule in default.keys():
        Config.set("default_settings",rule , default[rule])

    Config.write(cfgfile)
    cfgfile.close()


    return render_template('config.html')


if __name__ == "__main__":
    app.run(debug = True)
