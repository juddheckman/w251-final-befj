from numpy import array
import pandas as pd
import time
import csv
import sys
from scipy.sparse import hstack,issparse,vstack
import numpy as np
    
#Reads numrows of csvfiles and returns dataframe dfTrain
def readCSV(fileName,numrows):
    begintime = time.time()

    
    dfTrain = pd.read_csv(fileName, sep=",", header=False, encoding = 'utf8')

    endtime = time.time()
    totaltime = endtime-begintime
    print "csv reading time: " + str(totaltime) + "\n"


    return dfTrain

if __name__ == "__main__":
    df = readCSV('citi_logistic.csv', 5)
    #print df
    print str(df.values[0][2])
    print
    avgPrice = df.price.values
    t1 = df['price_t-1'].values
    t2 = df['price_t-2'].values
    t3 = df['price_t-3'].values
    #X = np.hstack((avgPrice, t1, t2, t3))
    #X = df[['AVG_PRICE', 't1', 't2', 't3']]
    X = df.as_matrix(['price', 'price_t-1', 'price_t-2', 'price_t-3'])
    print "X: ", X

    Y = df.price_dir_label.values
    print "Y: ", Y

    zipped = zip(X,Y)
    print "zipped: ", zipped

    print zipped[0][0]
    print zipped[0][1]
