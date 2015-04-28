from numpy import array
import pandas as pd
import time
import csv
import sys
    
#Reads numrows of csvfiles and returns dataframe dfTrain
def readCSV(fileName,numrows):
    begintime = time.time()

    
    dfTrain = pd.read_csv(fileName, sep=",", header=False, encoding = 'utf8',nrows = numrows)

    endtime = time.time()
    totaltime = endtime-begintime
    print "csv reading time: " + str(totaltime) + "\n"


    return dfTrain

if __name__ == "__main__":
    df = readCSV('samp50_jh.csv', 5)
    print df
    print str(df.values[0][2])