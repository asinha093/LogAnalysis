__author__ = 'abhinav sinha'
#imports json file to mongodb database

import json
from pymongo import MongoClient
import datetime

def log_to_mongodb(file_with_path):
	start_time = datetime.datetime.now()
	client = MongoClient()
	db = client.parselog
	#creates a database named parselog
	collection = db.NASA_Aug95
	#creates a collection named NASA_Aug95
	text = open(file_with_path, 'r').read()
	loadjson = json.loads(text)
	for item in loadjson:
		collection.insert(item)
		#insert the data to collection
	cursor = collection.find()
	print "Log entries imported: %s" % cursor.count()
	current_time = datetime.datetime.now()		
	print "Time Elapsed: %s" % str(current_time - start_time) 
	file_.close()
#end of function
print "Importing data..."
log_to_mongodb('/home/abhinav/Downloads/NASA_Aug95.json')

#or run the following command from terminal inside the directory where the file is located
#mongoimport --db <db name> --collection <collection name> --type json --file <filename>.json --jsonArray