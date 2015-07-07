'''
This file calculates the predicted values on a time interval using the data inserted in the table: traffic.
The file stores these predicted values (a list) into a new cassandra table: main_counts which will be used for building the UI !
'''
from cassandra.cluster import Cluster
from cassandra.query import panda_factory
import numpy as np
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter1d
import warnings
import uuid

class initialize(object):

	def __init__(self, keyspace, source, dest, host, port):
		# initializing the class variables
		self.keyspace = keyspace
		self.source = source
		self.dest = dest
		self.host = host
		self.port = port
		
	def initialize_connection(self):

		session = Cluster(contact_points=[self.host], port=self.port).connect(keyspace=self.keyspace)
		session.row_factory = panda_factory
		query = "SELECT epoch_time, post_count, byte_transfer, get_count, requests, visits FROM "+self.source
		DataFrame = session.execute(query).sort(columns=['epoch_time', 'post_count', 'byte_transfer', 'get_count', 'requests', 'visits'], ascending=[1,0,0,0,0,0])
		process = retrieve_insert(DataFrame, session, self.dest) # create instance for the class retrieve_insert
		process.retrieve_variables()
		return 1

class retrieve_insert(object):

	def __init__(self, DataFrame, session, dest):

		self.DataFrame = DataFrame
		self.session = session
		self.dest = dest

	def insert_prediction(self, time_sort, gets_orig, gets_pred, posts_orig, posts_pred, bytes_orig, bytes_pred, visits_orig, visits_pred, requests_orig, requests_pred):

		self.session.execute("CREATE TABLE IF NOT EXISTS "+self.dest+"(id uuid, link_error400 list<varchar>, link_error500 list<varchar>, time_error400 list <varchar>, time_error500 list<varchar>, country list<varchar>, country_count list<int>, time_sorted list<int>, gets_original list<int>, gets_predicted list<int>, posts_original list<int>, posts_predicted list<int>, bytes_original list<int>, bytes_predicted list<int>, visits_original list<int>, visits_predicted list<int>, requests_original list<int>, requests_predicted list<int>, vm_list list<varchar>, vm_count list<int>, req_list list<varchar>, req_count list<int>, os_list list<varchar>, os_count list<int>, device_list list<varchar>, device_count list<int>, last_timeid int, last_logid int, PRIMARY KEY (id))")

		self.session.execute("INSERT INTO "+self.dest+"(id, time_sorted, gets_original, gets_predicted, posts_original, posts_predicted, bytes_original, bytes_predicted, visits_original, visits_predicted, requests_original, requests_predicted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(uuid.uuid1(), time_sort, gets_orig, gets_pred, posts_orig, posts_pred, bytes_orig, bytes_pred, visits_orig, visits_pred, requests_orig, requests_pred))
		return 1

	def retrieve_variables(self):

		df = self.DataFrame
		# initialize target variable
		time = np.array([x[1] for x in enumerate(df['epoch_time'])])
		length = len(time)
		# initialize training variable
		get = np.array([x[1] for x in enumerate(df['get_count'])])
		byte = np.array([x[1] for x in enumerate(df['byte_transfer'])])
		post = np.array([x[1] for x in enumerate(df['post_count'])])
		request = np.array([x[1] for x in enumerate(df['requests'])])
		visit = np.array([x[1] for x in enumerate(df['visits'])])
		# remove noises from the fields
		# function calls
		remove = remove_noise_anomaly() # create instance for the class remove_noise_anomaly
		value = remove.remove_noise(get)
		count = value[2]
		indices = value[1]
		get = value[0]
		if count > 0:
			# remove anomalies from the fields
			post = remove.remove_anomaly(post, indices)
			byte = remove.remove_anomaly(byte, indices)
			request = remove.remove_anomaly(request, indices)
			visit = remove.remove_anomaly(visit, indices)
		# call predict functions
		warnings.simplefilter('ignore', np.RankWarning)
		predict = forecast() # create instance for the class forecast
		get_values = predict.predict_get(time, get, length)
		post_values = predict.predict_post(time, post, length)
		byte_values = predict.predict_byte(time, byte, length)
		req_values = predict.predict_request(time, request, length)
		visit_values = predict.predict_visit(time, visit, length)	
		# insert the predicted values to cassandra
		self.insert_prediction(time.tolist(), get_values[0].tolist(), get_values[1].tolist(), post_values[0].tolist(), post_values[1].tolist(), byte_values[0].tolist(), byte_values[1].tolist(), visit_values[0].tolist(), visit_values[1].tolist(), req_values[0].tolist(), req_values[1].tolist())
		return 1

class remove_noise_anomaly(object):

	def __init__(self):

		pass

	def remove_anomaly(self, field, indices):

		mean = np.mean(field[indices[-1]+1:])
		noisymean = np.random.normal(mean,2,len(indices)).tolist()
		np.put(field, indices, noisymean)
		return field

	def remove_noise(self, GET):

		indices = []
		count = 0
		for index in range(0, len(GET)):
			if GET[index] > 2*np.mean(GET[0:]):
				indices.append(index)
				count += 1
		if count > 0:
			mean = np.mean(GET[indices[-1]+1:])
			noisymean = np.random.normal(mean,2,len(indices)).tolist()
			np.put(GET, indices, noisymean)
		return GET, indices, count

class forecast(object):

	def __init__(self):

		pass

	def predict_get(self, x, y, l):
		
		plt.plot(x[:int(0.70*l)], y[:int(0.70*l)], 'co', ms = 3.5)
		x_new = x[1+int(0.70*l):]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_pred = []
		for values in y_new:
			y_pred.append(int(values))
		y_pred = gaussian_filter1d(np.array(y_pred), 5)
		plt.plot(x_new, y_pred, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total GET requests')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x_new[-1]])
		plt.show()
		return y, y_pred

	def predict_post(self, x, y, l):

		plt.plot(x[:int(0.70*l)], y[:int(0.70*l)], 'bo', ms = 3.5)
		x_new = x[1+int(0.70*l):]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_pred = []
		for values in y_new:
			y_pred.append(int(values))
		y_pred = gaussian_filter1d(np.array(y_pred), 5)
		plt.plot(x_new, y_pred, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total POST requests')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_pred

	def predict_byte(self, x, y, l):

		plt.plot(x[:int(0.70*l)], y[:int(0.70*l)], 'go', ms = 3.5)
		x_new = x[1+int(0.70*l):]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_pred = []
		for values in y_new:
			y_pred.append(int(values))
		y_pred = gaussian_filter1d(np.array(y_pred), 5)
		plt.plot(x_new, y_pred, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total Bytes transfered')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_pred

	def predict_request(self, x, y, l):

		plt.plot(x[:int(0.70*l)], y[:int(0.70*l)], 'ro', ms = 3.5)
		x_new = x[1+int(0.70*l):]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_pred = []
		for values in y_new:
			y_pred.append(int(values))
		y_pred = gaussian_filter1d(np.array(y_pred), 5)
		plt.plot(x_new, y_pred, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total Requests')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_pred

	def predict_visit(self, x, y, l):

		plt.plot(x[:int(0.70*l)], y[:int(0.70*l)], 'co', ms = 3.5)
		x_new = x[1+int(0.70*l):]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_pred = []
		for values in y_new:
			y_pred.append(int(values))
		y_pred = gaussian_filter1d(np.array(y_pred), 5)
		plt.plot(x_new, y_pred, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total Visits')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_pred