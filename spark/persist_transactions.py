# persist_transactions - SafePay spark streaming job (initial data load)

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
from datetime import datetime
import uuid

# spark
sc = SparkContext(appName="transactions1")
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 2)
spark = SparkSession(sc)

def processRDD(rdd):
 
    if rdd.isEmpty():
        return

    rdd2 = rdd.map(lambda x: str(uuid.uuid4()) + ";" + x)
    rdd3 = rdd2.map(lambda x: x.split(";"))
  
    # persist transactions in tx table for further proessing by a batch job
    
    df = rdd3.toDF(['tx_id', 'source', 'sent_dt', 'tx_dt', 'from_party_id', 'from_party_name', 'to_party_id', 'to_party_name'])

    df = df.drop('source')\
        .drop('sent_dt')\
        .withColumn('denied', lit(False))\
        .withColumn('reason', lit('passthru')) 
    
    df.write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(table="tx", keyspace="p2p")\
        .save()

# kafka
topic = "transactions1"
brokers_dns_str = "0.0.0.0:9092"

kvs=KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers_dns_str})

# read stream
batch=kvs.map(lambda x: x[1])
batch.count().map(lambda x:'Messages in this batch: %s' % x).pprint()

batch.foreachRDD(processRDD)

ssc.start()
ssc.awaitTermination()
