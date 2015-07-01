__author__ = 'abhinav'
'''
This file calculates the predicted values on a time interval using the data inserted in the table: traffic.
The file stores these predicted values (a list) into a new cassandra table: main_counts which will be used for building the UI !
'''
from cassandra.cluster import Cluster
from cassandra.query import panda_factory
import numpy as np
import matplotlib.pyplot as plt
from math import factorial
from datetime import datetime
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

		self.session.execute("CREATE TABLE IF NOT EXISTS "+self.dest+"(id uuid, country list<varchar>, country_count list<int>, time_sorted list<int>, gets_original list<int>, gets_predicted list<int>, posts_original list<int>, posts_predicted list<int>, bytes_original list<int>, bytes_predicted list<int>, visits_original list<int>, visits_predicted list<int>, requests_original list<int>, requests_predicted list<int>, vm_list list<varchar>, vm_count list<int>, req_list list<varchar>, req_count list<int>, os_list list<varchar>, os_count list<int>, device_list list<varchar>, device_count list<int>, last_timeid int, last_logid int, PRIMARY KEY (id))")

		self.session.execute("INSERT INTO "+self.dest+"(id, time_sorted, gets_original, gets_predicted, posts_original, posts_predicted, bytes_original, bytes_predicted, visits_original, visits_predicted, requests_original, requests_predicted) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",(uuid.uuid1(), time_sort, gets_orig, gets_pred, posts_orig, posts_pred, bytes_orig, bytes_pred, visits_orig, visits_pred, requests_orig, requests_pred))
		return 1

	def retrieve_variables(self):

		df = self.DataFrame
		# initialize target variable
		time = np.array([x[1] for x in enumerate(df['epoch_time'])])
		length = len(time)
		# initialize training variable
		get_ = np.array([x[1] for x in enumerate(df['get_count'])])
		byte_ = np.array([x[1] for x in enumerate(df['byte_transfer'])])
		post_ = np.array([x[1] for x in enumerate(df['post_count'])])
		request_ = np.array([x[1] for x in enumerate(df['requests'])])
		visit_ = np.array([x[1] for x in enumerate(df['visits'])])
		# remove noises from the fields
		# function calls
		remove = remove_noise_anomaly() # create instance for the class remove_noise_anomaly
		value = remove.remove_noise(get_)
		indices = value[1]
		get = value[0]
		# remove anomalies from the fields
		post = remove.remove_anomaly(post_, indices)
		byte = remove.remove_anomaly(byte_, indices)
		request = remove.remove_anomaly(request_, indices)
		visit = remove.remove_anomaly(visit_, indices)
		# call predict functions
		warnings.simplefilter('ignore', np.RankWarning)
		predict = forecast() # create instance for the class forecast
		get_values = predict.predict_get(time, get_, length)
		post_values = predict.predict_post(time, post_, length)
		byte_values = predict.predict_byte(time, byte_, length)
		req_values = predict.predict_request(time, request_, length)
		visit_values = predict.predict_visit(time, visit_, length)	
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
		for index in range(0,len(GET)):
			if GET[index] > 1500:
				indices.append(index)
		mean = np.mean(GET[indices[-1]+1:])
		noisymean = np.random.normal(mean,2,len(indices)).tolist()
		np.put(GET, indices, noisymean)
		return GET, indices

class forecast(object):

	def __init__(self):

		pass

	def smoothe_curve(self, y, window_size, order, deriv=0, rate=1):

	    try:
	        window_size = np.abs(np.int(window_size))
	        order = np.abs(np.int(order))
	    except ValueError, msg:
	        raise ValueError("window_size and order have to be of type int")
	    if window_size % 2 != 1 or window_size < 1:
	        raise TypeError("window_size size must be a positive odd number")
	    if window_size < order + 2:
	        raise TypeError("window_size is too small for the polynomials order")
	    order_range = range(order+1)
	    half_window = (window_size -1) // 2
	    # precompute coefficients
	    b = np.mat([[k**i for i in order_range] for k in range(-half_window, half_window+1)])
	    m = np.linalg.pinv(b).A[deriv] * rate**deriv * factorial(deriv)
	    # pad the signal at the extremes with
	    # values taken from the signal itself
	    firstvals = y[0] - np.abs( y[1:half_window+1][::-1] - y[0] )
	    lastvals = y[-1] + np.abs(y[-half_window-1:-1][::-1] - y[-1])
	    y = np.concatenate((firstvals, y, lastvals))
	    return np.convolve( m[::-1], y, mode='valid')

	def predict_get(self, x, y, l):
		
		plt.plot(x[:0.70*l], y[:0.70*l], 'co', ms = 3.5)
		x_new = x[1+0.70*l:]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_smooth = self.smoothe_curve(y_new, 251, 7)
		y_pred = []
		for values in y_smooth:
			y_pred.append(int(round(values, 0)))
		y_predic = np.array(y_pred)
		plt.plot(x_new, y_predic, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total GET requests')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_predic

	def predict_post(self, x, y, l):

		plt.plot(x[:0.70*l], y[:0.70*l], 'bo', ms = 3.5)
		x_new = x[1+0.70*l:]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_smooth = self.smoothe_curve(y_new, 351, 6)
		y_pred = []
		for values in y_smooth:
			y_pred.append(int(round(values, 0)))
		y_predic = np.array(y_pred)	
		plt.plot(x_new, y_predic, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total POST requests')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_predic

	def predict_byte(self, x, y, l):

		plt.plot(x[:0.70*l], y[:0.70*l], 'go', ms = 3.5)
		x_new = x[1+0.70*l:]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_smooth = self.smoothe_curve(y_new, 201, 6)
		y_pred = []
		for values in y_smooth:
			y_pred.append(int(round(values, 0)))
		y_predic = np.array(y_pred)	
		plt.plot(x_new, y_predic, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total Bytes transfered')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_predic

	def predict_request(self, x, y, l):

		plt.plot(x[:0.70*l], y[:0.70*l], 'go', ms = 3.5)
		x_new = x[1+0.70*l:]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_smooth = self.smoothe_curve(y_new, 201, 6)
		y_pred = []
		for values in y_smooth:
			y_pred.append(int(round(values, 0)))
		y_predic = np.array(y_pred)	
		plt.plot(x_new, y_predic, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total Requests')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_predic

	def predict_visit(self, x, y, l):

		plt.plot(x[:0.70*l], y[:0.70*l], 'go', ms = 3.5)
		x_new = x[1+0.70*l:]
		yinterp = np.interp(x_new, x, y)
		y_new = np.array(yinterp)
		y_smooth = self.smoothe_curve(y_new, 201, 6)
		y_pred = []
		for values in y_smooth:
			y_pred.append(int(round(values, 0)))
		y_predic = np.array(y_pred)	
		plt.plot(x_new, y_predic, 'k--', lw = 2.5)
		plt.xlabel('Time')    
		plt.ylabel('Total Visits')    
		plt.title('Trend Analysis')    
		plt.xlim([x[0], x[-1]])
		plt.show()
		return y, y_predic