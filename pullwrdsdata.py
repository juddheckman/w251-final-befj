
"""
Created on Wed Mar 18 23:16:54 2015

@author: bradmcminn
"""

import sys
import subprocess
import time


def main(argv):      
	
	shortyr = ""
	months = 0
	filecounter = 1
	filestoget = 10000

	if len(argv) == 1:
		shortyr = argv[0][2:4]
		months = argv[0][5:6]
	elif len(argv) == 2:
		filestoget = argv[1]	
		shortyr = argv[0][2:4]
		months = argv[0][5:6]
	else:
		print "Usage: pulldownwords.py <indexfile> <[optional]days_to_pull>"
		sys.exit(1)

	print "shortyr " + str(shortyr)
	print "months " + str(months)
	thefilename = "20" + str(shortyr) + "_" + str(months) + "mos_idx.txt"	
	
	print "Opening index file: " + thefilename
	
	f = open(thefilename, 'r')
	for line in f:
		filecounter+=1	
		print "Downloading data file: " + line
		print line[31:51]
		starttime = time.time()
		# Note that you need proper authenticaion to be able to access the WRDS repository
		thescp = subprocess.Popen(['scp', 'friehl@wrds-cloud.wharton.upenn.edu:'+line, "/data/" + line[31:51]], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		scpout, err =  thescp.communicate()
		endtime = time.time()
		print "elapsed time for file " + str((endtime - starttime) * 1000) + "seconds"
		if int(filecounter)>int(filestoget):
			break
		
		
	print "Done"

if __name__ == "__main__":
    main(sys.argv[1:])
