__author__ = 'abhinav sinha'
#extracts data from logfile to a json file

import re
from datetime import datetime

#logformat
#in24.inetnebr.com - - [01/Aug/1995:00:00:01 -0400] "GET /shuttle/missions/sts-68/sts-68-mcc-05.txt HTTP/1.0" 200 1839

w = open('/home/abhinav/Downloads/NASA_Aug95.json', 'w')
#write a json file containing the extracted data

def extract_data(file_with_path):
    #function to extract data from the log file

    r = open(file_with_path)
    textfile = r.readlines()
    i=0
    start_time = datetime.now()
    w.write('[\n')

    for line in textfile:
        text = line

        if(i > 0):
            w.write(',\n{\n')
        else:
            #the first line of each data group starts with {
            w.write('{\n')
#host_data
        try:
            host = re.search('(.+?) - -',text).group(1)
            w.write('"host": "%s", \n' % host)
	    #write the host_data in json format
        except:
            host = "-"
            
#timestamp_details
        try:
            loc1 = text.find('[')
            loc2 = text.find(']')
            time = text[loc1+1:loc2]
            date = time.split(':')[0]
            timestamp = time[time.find(':')+1:time.find(':')+9]
            date = datetime.strptime(date, '%d/%b/%Y').strftime('%d.%m.%Y')+" "+timestamp
            w.write('"timestamp": "%s", \n' % date)
	    #write the timestamp data in json format
        except ValueError:
            date = "-"
 
#request            
	#request_line
        try:
            loc1 = text.find(' "')
            loc2 = text.find('" ')
            request = text[loc1+2:loc2]
            w.write('"request": {"requestline": "%s",' % request)
	    #write the request_line in json format
        except:
            request = "-"
            
	#status_code
        try:
            loc = text.find('" ')
            status = text[loc+2:]

            if " " in status:
                status = (text[loc+1:].split(" "))[1]
                w.write('"statuscode": "%s"}, \n' % status)
		#write the status_code data in json format
            else:
                s = int(status)

                if s > 999:
		    #dealing with error in file format--no space between the status_code and byte_size data!!
                    s = s/10**(len(status)-4)
		    #extracting the status_code from the combined number 
                    status = str(s)
                    w.write('"statuscode": "%s"}, \n' % status)
		    #write the status_code data in json format
        except:
            status = "-"

#bytesize
        try:
            loc = text.find('" ')
            size = (text[loc:].split(" "))[1]
            s = int(size)
            if s > 999:
		#dealing with error in file format--no space between the statuscode and bytesize data!!
                bytes = size.split('00')[1].split('\n')[0]
                w.write('"bytes": "%s" \n}' % bytes)
		#write the byte_size data in json format
            else:
                bytes = (text[loc:].split(" "))[2].split('\n')[0]
                w.write('"bytes": "%s" \n}' % bytes)
		#write the byte_size data in json format
        except:
            bytes = "-"

        i = i+1
    #end of for loop
    
    print "Total %s entries" % i
    current_time = datetime.now()    
    print "Time elapsed: %s" % str(current_time - start_time)
    w.write(']')
#end of function
print "extracting data..."
print "creating json file.."
extract_data('/home/abhinav/Downloads/NASA_Aug95')
w.close()