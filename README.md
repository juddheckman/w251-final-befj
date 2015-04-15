# w251-final-befj

## MLlib prototype
 - intial_model.py: reads in feature and label data from csv, convert to rdd, train model using spark
 - config.py: spark configuration information used by initial_model.py
 - sample_500mm_avg_t10_labels_A: sample training data for stock A

###sample execution: 
`/usr/local/spark/bin/spark-submit initial_model.py spark://bjm-spark-1:7077 bjm-spark-1 sample_500mm_avg_t10_labels_A.csv 2800m 1000