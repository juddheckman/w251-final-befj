from pyspark import SparkConf, SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD, SVMWithSGD
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD
from pyspark.storagelevel import StorageLevel
from numpy import array
import config
#import zoneold
import pandas as pd
import time
import csv
import sys
#from scipy.sparse import hstack,issparse,vstack
from sklearn.feature_extraction.text import TfidfVectorizer,HashingVectorizer
from sklearn.cross_validation import train_test_split
from sklearn.metrics import classification_report,f1_score,accuracy_score
from sklearn.linear_model import SGDClassifier
from math import ceil

#Parameters for model
learningRate = .00001
cParam = 1.0
numIterations = 30
##################
    
#Reads numrows of csvfiles and returns dataframe dfTrain
def readCSV(fileName):
    begintime = time.time()

    dfTrain = pd.read_csv(fileName, sep=",", header=False, encoding = 'utf8')

    endtime = time.time()
    totaltime = endtime-begintime
    print "csv reading time: " + str(totaltime) + "\n"

    return dfTrain

#Iterates through dfTrain, building the Y vector
def buildY(dfTrain):

    #labelName = 'price_dir_label'
    labelName = 'volume_dir_label'
    #labelName = 'tnext_dir'
    #labelName = 'tnext'

    Y = dfTrain[labelName].values

    return Y

#Iterates through dfTrain, building the X matrix
def buildX(dfTrain):
    begintime = time.time()

    #features = ['price', 'price_t-1', 'price_t-2', 'price_t-3', 'volume', 'volume_t-1', 'volume_t-2', 'volume_t-3']
    features = ['price', 'price_t-1', 'price_t-2', 'volume10k', 'volume10k_t-1']
    #features = ['AVG_PRICE', 't1', 't2', 't3']
    print "features: " + ', '.join(features)

    X = dfTrain.as_matrix(features)

    endtime = time.time()
    totaltime = endtime-begintime
    print "building x time: " + str(totaltime)

    return X

#Splits dataset into train and test (so that we can evaluate the model)
def split(X,Y):
    Xtrain,Xtest,Ytrain,Ytest = train_test_split(X, Y, test_size=0.20)
    return Xtrain,Xtest,Ytrain,Ytest

#Trains and return the model
def buildModel(trainrdd):
    
    model = LogisticRegressionWithSGD.train(trainrdd)
    #model = LinearRegressionWithSGD.train(trainrdd)
    return model

#Returns prediction of whether post should be closed or open for a single post
def getPredictions(singleXtest):
    
    return model.predict(singleXtest)

#Iterates through test vectors, predicting output category for each one
#Then compares to true labels and prints evaluation metrics
def handlePrediction(model,Xtest,Ytest):
    predicted = map(getPredictions,Xtest)
    
    print "multiclass"
    print classification_report(Ytest, predicted)
    
    print "overall accuracy: " + str(accuracy_score(Ytest, predicted, normalize=True))

#Builds a single LabeledPoint out of tuple of an x row and corresponding y value
#Spark requires a certain format--hence transposing and sorting
def parsePoint(trainingTuple):
    return LabeledPoint(trainingTuple[1], trainingTuple[0])


#Builds Spark rdd out of X and Y 
def buildLabeledPoints(X,Y):
    trainingTuples = zip(X,Y)

    labeledPoints=map(parsePoint,trainingTuples)

    rdd = sc.parallelize(labeledPoints)
    return rdd


if __name__ == "__main__":
    begintime = time.time()
    
    config.setupSpark()

    if len(sys.argv) !=6 or sys.argv[1] == "-h":
        print "Usage: initial_model.py <sparkloc master> <sparkhost> <executor memory> <datafile> <num posts (or rows)>"
        sys.exit()

    sparkloc = sys.argv[1] # local for local
    sparkhost = sys.argv[2] # localhost for local
    fileName = sys.argv[3] 
    
    execmem = sys.argv[4]
    
    numrows = int(sys.argv[5])

    conf = (SparkConf()
         .setMaster(sparkloc)
         .setAppName("My app")
         .setSparkHome("/usr/local/spark")
         .set("spark.driver.host", sparkhost)
         .set("spark.executor.memory", execmem))
    sc = SparkContext(conf = conf)

    fulltrainrdd = None
    xtestagg = []
    fullytest = []
    
    dfTrain = readCSV(fileName)
        
    Y = buildY(dfTrain)

    X = buildX(dfTrain)

    Xtrain,Xtest,Ytrain,Ytest = split(X,Y)
    print "data split............."

    trainrdd = buildLabeledPoints(Xtrain,Ytrain)
    print "rdd built..........."

    print trainrdd.count()

    model = buildModel(trainrdd)
    print "model built..........."

    handlePrediction(model,Xtest,Ytest)
    print "prediction handled..........."
    
    endtime = time.time()
    totaltime = endtime-begintime
    print "total time: " + str(totaltime)
    
    sc.stop() 
