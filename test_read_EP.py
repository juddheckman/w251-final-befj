import pandas as pd
import numpy as np

import datetime
import time
import csv
import sys
    
#Reads numrows of csvfiles and returns dataframe dfTrain
def readCSV(fileName,numrows=5):

    dfTrain = pd.read_csv(fileName, 
        sep=",", 
        header=False, 
        encoding = 'utf8',
        nrows = numrows)

    return dfTrain
    
def transformTime(df):

    #turning SAS date time into timestamps
    epoch = datetime.datetime(1960,1,1)
    df['DATE2'] = df['DATE'].apply(
        lambda d: epoch + datetime.timedelta(days=int(d))
    ) 

    df.DATE2.values.astype('M8[D]')

    #turning datetime.time(h,m,s) into timestamps
    df['TIME2'] = df['TIME'].apply(
        lambda d: pd.datetime.strptime(d[15:len(d)-1] 
            if d.count(',')>1 else d[15:len(d)-2]+',0','%H, %M, %S')
    )

    #getting rid of unneecssary columns.  
    #I couldn't find a difference btween pop/drop, one might still be in memory
    df.pop('TIME')
    df.pop('DATE')
    df=df.drop(['G127', 'CORR', 'COND', 'EX'], axis=1)

    return df
    
def groupData(df):
    begintime = time.time()

    #grouping by; averaging and summing price and size
    out = df.groupby(['SYMBOL', 'TIME2', 'DATE2'], as_index=False).agg(
        {'PRICE': [np.mean], 
        'SIZE': [np.sum]})

    #renaming columns
    out.columns = ['SYMBOL', 'TIME2', 'DATE2', 'AvgPrice', 'TotSize']

    

    return out

#this function only ads t-1 and t-2.  
#more can be added manually (and probably done elegantly to loop)
def timeIndex(df):

    #adding previous times  add more if needed
    df['T1_P']= ""
    df['T2_P']= ""

    df['T1_V']= ""
    df['T2_V']= ""

    #iterating over rows, will need ot adjust based on number of past times needed
    for i, r in df.iterrows():
        if i>0:
            if df.irow(i)['SYMBOL']== df.irow(i-1)['SYMBOL']:                
                df.ix[i,'T1_P']=df.irow(i-1)['AvgPrice']
                df.ix[i,'T1_V']=df.irow(i-1)['TotSize']

        if i>1:
            if df.irow(i)['SYMBOL']== df.irow(i-2)['SYMBOL']:                
                df.ix[i,'T2_P']=df.irow(i-2)['AvgPrice']
                df.ix[i,'T2_V']=df.irow(i-2)['TotSize']

    return df


if __name__ == "__main__":
    df = readCSV('samp50_jh.csv', 50)  #param for number of rows used for testing
    df = transformTime(df)
    agg = groupData(df)
    print agg
    final=timeIndex(agg)
    print final
||||||| merged common ancestors
=======
import pandas as pd
import numpy as np

import datetime
import time
import csv
import sys
    
#Reads numrows of csvfiles and returns dataframe dfTrain
def readCSV(fileName,numrows=5):

    dfTrain = pd.read_csv(fileName, 
        sep=",", 
        header=False, 
        encoding = 'utf8',
        nrows = numrows)

    return dfTrain
    
def transformTime(df):

    #turning SAS date time into timestamps
    epoch = datetime.datetime(1960,1,1)
    df['DATE2'] = df['DATE'].apply(
        lambda d: epoch + datetime.timedelta(days=int(d))
    ) 

    df.DATE2.values.astype('M8[D]')

    #turning datetime.time(h,m,s) into timestamps
    df['TIME2'] = df['TIME'].apply(
        lambda d: pd.datetime.strptime(d[15:len(d)-1] 
            if d.count(',')>1 else d[15:len(d)-2]+',0','%H, %M, %S')
    )

    #getting rid of unneecssary columns.  
    #I couldn't find a difference btween pop/drop, one might still be in memory
    df.pop('TIME')
    df.pop('DATE')
    df=df.drop(['G127', 'CORR', 'COND', 'EX'], axis=1)

    return df
    
def groupData(df):
    begintime = time.time()

    #grouping by; averaging and summing price and size
    out = df.groupby(['SYMBOL', 'TIME2', 'DATE2'], as_index=False).agg(
        {'PRICE': [np.mean], 
        'SIZE': [np.sum]})

    #renaming columns
    out.columns = ['SYMBOL', 'TIME2', 'DATE2', 'AvgPrice', 'TotSize']

    return out

#this function only ads t-1 and t-2.  
#more can be added manually (and probably done elegantly to loop)
def timeIndex(df):

    #adding previous times  add more if needed
    df['T1']= ""
    df['T2']= ""

    #iterating over rows, will need ot adjust based on number of past times needed
    for i, r in df.iterrows():
        if i>0:
            if df.irow(i)['SYMBOL']== df.irow(i-1)['SYMBOL']:                
                df.ix[i,'T1']=df.irow(i-1)['AvgPrice']

        if i>1:
            if df.irow(i)['SYMBOL']== df.irow(i-2)['SYMBOL']:                
                df.ix[i,'T2']=df.irow(i-2)['AvgPrice']

    return df


if __name__ == "__main__":
    df = readCSV('samp50_jh.csv', 50)  #param for number of rows used for testing
    df = transformTime(df)
    agg = groupData(df)
    final=timeIndex(agg)
    print final

