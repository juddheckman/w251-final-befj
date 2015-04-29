
"""
Created on Tue Mar 17 21:52:20 2015

@author: bradmcminn
"""

import sys
import subprocess

def main(argv):      
	
	shortyr = ""
	months = 0
	monthcounter = 1
	directories = []
	files = []

	if len(argv) == 2:
		shortyr = argv[0][2:4]
		months = argv[1]
	else:
		print "Usage: pulldownwords.py <calendar yr> <months to pull>"
		sys.exit(1)
		
	print "Beginning of indexing of " + str(months) + " months of data from year " + "20" + str(shortyr) + "..." 
                  
	ls = subprocess.Popen(['ssh','friehl@wrds-cloud.wharton.upenn.edu', 'ls', '/wrds/taq.20' + shortyr ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	lsout, err =  ls.communicate()
	lslines = lsout.splitlines()
	for line in lslines:
		directories.append(line)

	
	for directory in directories:
		print "Indexing directory for month " + str(monthcounter) +": " + directory + "..."
		monthcounter+=1
		# Note that you need proper authenticaion to be able to access the WRDS repository
		ls = subprocess.Popen(['ssh','friehl@wrds-cloud.wharton.upenn.edu', 'ls', '/wrds/taq.20' + shortyr + '/' + directory + '/sasdata'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		lsout, err =  ls.communicate()
		lslines = lsout.splitlines()
		for line in lslines:
			if line[0:2] == "ct" and line[16:20] == "bdat":
				files.append('/wrds/taq.20' + shortyr + '/' + directory + '/sasdata' + "/" + line)
		print "Done processing directory: " + directory 
		
		if int(monthcounter) > int(months):
			print "Finished indexing " + str(months) + " months of data"
			break
		
	f = open("20" + str(shortyr) + "_" + str(months) + "mos_idx.txt", 'w')
	for file in files:
		f.write(file+"\n")

if __name__ == "__main__":
    main(sys.argv[1:])
