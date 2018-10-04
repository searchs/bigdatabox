from nltk.tokenize import wordpunct_tokenize
from pyspark import SparkConf, SparkContext
import sys

"""Running commands:
Spark 2(local instance): spark-submit word_count.py local wiki.txt  wc-count
Spark 2(local instance): spark-submit word_count.py "local[4]" wiki.txt  wc-count

Spark 2(cluster instance): spark-submit word_count.py spark://dlrc-headnode:7077 \
                        hdfs://dlrc-headnode/wiki-txt hdfs://dlrc-headnode/users/devola/wc-output
                        
    # Default Configuration (Basic)
# from pyspark import SparkContext
# from pyspark.sql import SparkSession
# sc = SparkContext.getOrCreate()
# spark = SparkSession(sc)
        
"""

if len(sys.argv) < 4:
    print(">>sys.stderr", "Usage: %s master input output" % sys.argv[0])
    exit(1)

sc = SparkContext(sys.argv[1], "Word count")
file = sc.textFile(sys.argv[2])
counts =  file.flatMap(lambda x: wordpunct_tokenize(x)) \
            .map(lambda x: (x,1)) \
            .reduceByKey(lambda x,y: x + y)

counts.map(lambda (x,y) : "%s \t%d" % (x,y)) \
       .saveAsTextFile(sys.argv[3])
