import time
import subprocess
import sys
from sas7bdat import SAS7BDAT
import socket
import random
import object_storage

sl_storage = object_storage.get_client(<<SOFTLAYER ID>>, <<SOFTLAYER KEY>>, datacenter=<<SOFTLAYER REGION>>)

# Set up paths to files
# Local Paths
local_path = "/data/"
hadoop_path = "/usr/local/hadoop/"
# HDFS Paths
hdfs_path_in = "/user/convert_in/"
hdfs_path_out = "/user/convert_out/"
hdfs_path_list = "/user/convert_list/"
# Object Store Paths
objstore_path_in = sl_storage['assign7_container3']['convert_in'] 
objstore_path_out = sl_storage['assign7_container3']['convert_out']
objstore_path_list = sl_storage['assign7_container3']['convert_list']

thishost = socket.gethostname()

listfilename = "2011_5mos_idx.txt"

timelogfilename = local_path + "timelog_" + time.strftime("%Y-%m-%d_%H-%M") + ".txt"
timelog = open(timelogfilename,'w') 
timelog.write("label\ttimestart\ttimeend\ttimedif\n")
timelog.close()

cumulconverttime = 0
overallstarttime = time.time()

logfilename = local_path + "processlog_" + time.strftime("%Y-%m-%d_%H-%M") + ".txt"
logfile = open(logfilename,'w')
logfile.close()

def convert_sas_file(sasfilename, csvfilename, tempfilename, storagetype):
	processfile = False
	# Grab the SAS file from the filestore
	if storagetype == 'hdfs':
		results = subprocess.Popen([hadoop_path + 'bin/hdfs','dfs', '-copyToLocal', hdfs_path_in + sasfilename, local_path + sasfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		commandout, err =  results.communicate()
		if len(err) <> 0:
			erroroccurred = True
		else:
			erroroccurred = False
	else:
		try:
                	slfile = objstore_path_in[sasfilename].read()
                	f = open(local_path + sasfilename,"wb")
                	f.write(slfile)
                	f.close()
			erroroccurred = False
		except:
			erroroccurred = True
	
	# Check to see if an error occurred grabbing the file from the filestore
	if erroroccurred:
		logfile = open(logfilename,'a')
		logfile.write(thishost + ": Error retrieving raw SAS file from file store: " + sasfilename + " -- " + str(err) + "\n")
		logfile.write(thishost + " "  + sasfilename + " " + local_path + sasfilename + "\n")
		logfile.close()
	else:
		logfile = open(logfilename,'a')
		logfile.write(thishost + ": Successfully retrieved raw SAS file from file store: " + sasfilename + "\n")
		logfile.close()
		processfile = True
	
	if processfile == True:
		# Use SAS7BDAT convert library to read in the file
		inputfile = SAS7BDAT(local_path + sasfilename)

		logfile = open(logfilename,'a')
		logfile.write(thishost + ": Successfully initialized SAS7BDAT converter with: " + local_path + sasfilename + "\n")
		logfile.close()

		outfile = open(local_path + csvfilename, "wb")

		# Format the line string to be csv formatted
		linecount = 0
		for row in inputfile:
			stringval = str(row)
			if linecount <> 0:
				stringval = str(stringval).replace('[','').replace(']','')
				stringval = stringval.replace(" ", "")
				stringval = stringval.replace("u'","").replace("'", "")
				startparenloc = stringval.find("(")
		        	endparenloc = stringval.find(")",startparenloc)
		        	timedigits = stringval[startparenloc+1:endparenloc]
		        	digits = timedigits.split(",")
		        	rejoineddigits = ""
		        	for digit in digits:
		        	        if len(digit) == 1:
		        	                digit = "0" + str(digit)
                			rejoineddigits = rejoineddigits + ":" + digit
        			rejoineddigits = rejoineddigits[1:]
        			stringval = stringval[:startparenloc+1] + stringval[endparenloc:]
        			stringval = stringval.replace('datetime.time(', rejoineddigits).replace(')', '')
 			else:
	                        stringval = str(stringval).replace('[','').replace(']','')
	                        stringval = stringval.replace(" ", "")
				stringval = stringval.replace("u'","").replace("'", "")
                        	stringval = stringval.replace('datetime', '"datetime').replace(')', ')"')
               	
			# Write out the row the converted, csv formatted row
			outfile.write(stringval+"\n")
			linecount+=1
                
			# DEBUG 
			if linecount > 10000:
				break
		logfile = open(logfilename,'a')		
		logfile.write(thishost + ": Finished converting to and creating csv file " + local_path + csvfilename + "\n")
		logfile.close()	
	
		outfile.close()
		
		# Write the resulting file back to filestore
		if storagetype == 'hdfs':
			
			results = subprocess.Popen([hadoop_path + 'bin/hdfs','dfs', '-copyFromLocal', local_path + csvfilename, hdfs_path_out + csvfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			outtext, err =  results.communicate()
		else:
			outfile = open(local_path + csvfilename,'rb')
			objstore_path_out[csvfilename].send(outfile)
			outfile.close()

		logfile = open(logfilename,'a')
		logfile.write(thishost + ": Successfully wrote converted csv back to filestore: " + hdfs_path_out + csvfilename + "\n")
       		logfile.close()

	# Remove the temporary local files
	results = subprocess.Popen(['rm',  local_path + sasfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	outtext, err =  results.communicate()
        
	results = subprocess.Popen(['rm',  local_path + csvfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outtext, err =  results.communicate()

        results = subprocess.Popen(['rm',  local_path + tempfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        outtext, err =  results.communicate()

	
	# Remove the temporary filestore files
        if storagetype == 'hdfs':
		results = subprocess.Popen([hadoop_path + 'bin/hdfs','dfs', '-rm', hdfs_path_out + tempfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        	outtext, err =  results.communicate()
	else:
		objstore_path_out[tempfilename].delete()
	
	logfile = open(logfilename,'a')
	logfile.write(thishost + ": Removed temporary files: " + local_path + sasfilename + ", " + csvfilename + "; " + hdfs_path_out + tempfilename + "\n")
	logfile.close()

def main(argv):

	storagetype = 'hdfs'

	if len(argv) != 1:
		print "Unknown storage type, exiting..."
		exit()
	else: 	
	        storagetype = argv[0]

	if storagetype != 'objectstore' and storagetype != 'hdfs':
		print "Unknown storage type, exiting..."
		exit()
	
	# Retrieve the file list to convert
	logfile = open(logfilename,'a')
	logfile.write("Storagetype: " + storagetype + "\n")
	logfile.write("Retrieving list..." + str(thishost) + "\n") 
	logfile.close()

	if storagetype == 'hdfs':
		results = subprocess.Popen([hadoop_path + 'bin/hdfs','dfs', '-copyToLocal', hdfs_path_list + listfilename, local_path + listfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        	commandout, err = results.communicate()
	else: 
	        slfile = objstore_path_list[listfilename].read()
		f = open(local_path + listfilename,"wb")
        	f.write(slfile)
        	f.close()
					
	filecount=0
	# Read the list file and iterate over the files to convert
	locallistfile = open(local_path + listfilename,"r")
	listoffiles = locallistfile.readlines()
	indexes = range(0,len(listoffiles))
	random.shuffle(indexes)

	for i in indexes:
		# Get arguments from the commandline, create filenames 
		convertstarttime = time.time()
		sasfilename = listoffiles[i][31:].strip()
		csvfilename = sasfilename[:11] + ".csv"
		tempfilesuffix = ".sascontmp"
		tempfilename = csvfilename + tempfilesuffix
		
		logfile = open(logfilename,'a')
		logfile.write(thishost + ":---------- Starting with: " + sasfilename + " ----------" + "\n")
		logfile.write(thishost + " -- Processing: " + sasfilename + "\n")
		logfile.close()		

		# Check to see if the csv has already been written to HDFS
		logfile = open(logfilename,'a')
		logfile.write(thishost + ": Checking to see if file exists in filestore: " + hdfs_path_out + tempfilename + "\n")
		logfile.close()

		filealreadyexists = False
		if storagetype == 'hdfs':
			results = subprocess.Popen([hadoop_path + 'bin/hdfs','dfs', '-count', hdfs_path_out + csvfilename + "*"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			commandout, err =  results.communicate()

			if len(commandout) !=0:
				filealreadyexists = True
			else:
				filealreadyexists = False
		else:
			files = sl_storage['assign7_container3'].objects()
			for thisfile in files:
				if csvfilename in str(thisfile):
					filealreadyexists = True
					break
				else:
					filealreadyexists = False

		# If the file has already been converted, move on to the next file
		if filealreadyexists:
			# Get the next file
			logfile = open(logfilename,'a')
			logfile.write(thishost + ": " + tempfilename + " -- File exists in filestore, so moving to next file... " + "\n")
			logfile.close()
		else:
			# Create a temporary file locally to copy to filestore denoting this file is being converted
			logfile = open(logfilename,'a')
			logfile.write(thishost + ": Converted file does not exist in filestore, copy temp file to HDFS" + hdfs_path_out + tempfilename + "\n")
			logfile.close()

			tempfile = open(local_path + tempfilename, "w")
			tempfile.close()
			tempfile = open(local_path  + tempfilename, "r")

			if storagetype == 'hdfs':
				results = subprocess.Popen([hadoop_path + 'bin/hdfs','dfs', '-copyFromLocal', local_path + tempfilename, hdfs_path_out + tempfilename], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                		outtext, err =  results.communicate()
			else:
		        	objstore_path_out[tempfilename].send(tempfile)

			# Pull the file down and run conversion, generate csv	
			convert_sas_file(sasfilename, csvfilename, tempfilename, storagetype)
		logfile = open(logfilename,'a')
		logfile.write(thishost + " ---------- Finished with " + csvfilename + " ----------\n" + "\n") 
		logfile.close()

		convertendtime = time.time()
		timelog = open(timelogfilename,'a')
		timelog.write(thishost + "-" + storagetype + ": " + csvfilename + "\t" + str(convertstarttime) + "\t" + str(convertendtime) + "\t" + str(convertendtime - convertstarttime) + "\n")
		timelog.close()

		# DEBUG
		filecount+=1
		#if filecount>10:
		#	break
	overallendtime = time.time()
	timelog = open(timelogfilename,'a')
	timelog.write(thishost + "-" + storagetype + ": overall\t" + str(overallstarttime) + "\t" + str(overallendtime) + "\t" + str(overallendtime - overallstarttime) + "\n")
	timelog.close()

	logfile = open(logfilename,'a')
	logfile.write('Done' + "\n")
	logfile.close()

if __name__ == "__main__":
    main(sys.argv[1:])

