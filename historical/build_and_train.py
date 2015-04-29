import time
import datetime
import config

import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

import pandas as pd
import numpy as np

def transform_time(t):
    datestring = '2011-01-03T'
    ds = datestring + t
    return time.mktime(datetime.datetime.strptime(ds, '%Y-%m-%dT%H:%M:%S').timetuple())


def build_features(fileName):
    #conf = SparkConf().setAppName('symbols')
    #sc = SparkContext(conf=conf)

    sqlContext = SQLContext(sc)
    data = sc.textFile(fileName).map(lambda line: line.split(","))
    rows = data.filter(lambda x: x[0] != 'SYMBOL')
    df = rows.map(lambda p: (p[0].strip(), transform_time(p[2].strip()), float(p[3].strip()), float(p[4].strip())))

    symbols = df.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaSymbols = sqlContext.inferSchema(symbols)
    schemaSymbols.registerTempTable("symbols")

    trades = sqlContext.sql("""SELECT symbol, time, sum(price*volume)/sum(volume) as avg_price, sum(volume) as volume from
            symbols group by symbol, time""")
    trades = trades.map(lambda x: Row(symbol=x[0], time=x[1], price=x[2], volume=x[3]))
    schemaTrades = sqlContext.inferSchema(trades)
    schemaTrades.registerTempTable("trades")

    # remove limit after test
    syms = sqlContext.sql("SELECT distinct symbol from trades")
    syms = syms.collect()

    df_dict = {}
    print type(syms)
    for sym in syms:
        sym = sym.symbol.strip()
        print sym
        sym_data = sqlContext.sql("SELECT symbol, time, price, volume FROM trades WHERE symbol = '{}' ORDER BY symbol, time".format(sym))

        sym_data = sym_data.collect()
        print len(sym_data)
        sym_df = pd.DataFrame(sym_data, columns=['symbol', 'time', 'price', 'volume'])
        for i in range(1,11):
            sym_df['price_t-'+str(i)] = sym_df['price'].shift(i)

        for i in range(1,11):
            sym_df['volume_t-'+str(i)] = sym_df['volume'].shift(i)

        # add labels for price and volume
        sym_df['price_label'] = sym_df['price'].shift(-1)
        sym_df['volume_label'] = sym_df['volume'].shift(-1)

        sym_df = sym_df.dropna()
        df_dict[sym] = sym_df
        print sym_df

    # print for testing
    print len(df_dict)
    print df_dict.keys()
    print type(df_dict[sym])
    sc.stop()

if __name__ == '__main__':
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

    build_features(fileName)

