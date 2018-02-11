# calculate_stats - SafePay spark batch job

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from cassandra.cluster import Cluster
import datetime
import uuid

# spark
sc = SparkContext(appName="Calculate Metrics")
spark = SparkSession(sc)
sc.setLogLevel('ERROR')

# cassandra
cluster = Cluster(['0.0.0.0', '0.0.0.0', '0.0.0.0'])

# datetime format used by cassandra's timestamp's data type
date_format = "%Y-%m-%d %H:%M:%S"

# processing window (days)
window = 1

# limits
chg_limit = 3.0
max_limit = 30

# read batch history and get last batch end date
df_ob = spark.read\
    .format('org.apache.spark.sql.cassandra')\
    .options(table='ops_batch', keyspace='p2p')\
    .load()

if df_ob.rdd.isEmpty():
    last_batch_end_dt = datetime.datetime.now() - datetime.timedelta(days=window)
else:
    row = df_ob.filter(df_ob.process_end_dt.isNotNull()).agg(max(df_ob.batch_end_dt)).first()
    last_batch_end_dt = row['max(batch_end_dt)']

# set current batch window boundaries
start_dt = last_batch_end_dt
end_dt = start_dt + datetime.timedelta(days=window)
start_str = start_dt.strftime(date_format)
end_str = end_dt.strftime(date_format)

# save batch processing start time
process_start_dt = datetime.datetime.now()

# read transactions table into a dataframe
df_tx = spark.read\
    .format('org.apache.spark.sql.cassandra')\
    .options(table='tx', keyspace='p2p')\
    .load()

# read from_party_stats (needed to check last_cnt and max_cnt against new data)
df_old = spark.read\
    .format('org.apache.spark.sql.cassandra')\
    .options(table='from_party_stats', keyspace='p2p')\
    .load()

# read to_party_stats (needed to check last_cnt and max_cnt against new data)
df_oldt = spark.read\
    .format('org.apache.spark.sql.cassandra')\
    .options(table='to_party_stats', keyspace='p2p')\
    .load()

# filter transacgtion data to work with this batch's window
df_win = df_tx\
    .filter((df_tx.tx_dt > start_str) & (df_tx.tx_dt < end_str))\
    .select('tx_dt', 'from_party_id', 'from_party_name', 'to_party_id', 'to_party_name')

# prepare from party and to party dataframes
df_fp = df_win\
    .select('from_party_id', 'from_party_name')
df_tp = df_win\
    .select('to_party_id', 'to_party_name')

# build a single list of unique parties in the batch window
df_fp1 = df_fp\
    .withColumnRenamed('from_party_id', 'party_id')\
    .withColumnRenamed('from_party_name', 'party_name')
df_tp1 = df_tp\
    .withColumnRenamed('to_party_id', 'party_id')\
    .withColumnRenamed('to_party_name', 'party_name')
df_p = df_fp1.union(df_tp1)\
    .dropDuplicates(['party_id'])

# aggregate last day's data to calculate from party stats
df_new = df_fp\
    .groupBy('from_party_id').count()

# aggregate last day's data to calculate to party stats
df_newt = df_tp\
    .groupBy('to_party_id').count()

# join new from party data persisted form the stream and old from party data read from the database
# column names
# df_old: from_party_id, last_cnt, max_cnt, chg_limit, max_limit, blacklisted, reason
# df_new: from_party_id, count
# df_upd: from_party_id, last_cnt, max_cnt, chg_limit, max_limit, blacklisted, reason, count

# update old records that have matching new records; ignore parties that already have been blacklisted
df_upd = df_old.alias('df_old')\
    .filter(df_old.blacklisted == 'False')\
    .join(df_new.alias('df_new'), 'from_party_id', 'inner')\
    .withColumn('last_cnt_', df_new['count'])\
    .withColumn('max_cnt_', greatest(df_old['max_cnt'], df_new['count']))\
    .withColumn('chg_limit_', df_old['chg_limit'])\
    .withColumn('max_limit_', df_old['max_limit'])\
    .withColumn('blacklisted_', (greatest(df_old['max_cnt'], df_new['count']) > df_old['max_limit']) | ((df_new['count']/df_old['last_cnt']) > df_old['chg_limit']))\
    .withColumn('reason_', struct('blacklisted_', lit('volume'), df_old['max_cnt'], df_new['count'], df_old['max_limit'], lit('acceleration'), df_new['count'], df_old['last_cnt'], df_old['chg_limit']))\
    .drop('last_cnt')\
    .drop('max_cnt')\
    .drop('chg_limit')\
    .drop('max_limit')\
    .drop('blacklisted')\
    .drop('reason')\
    .withColumnRenamed('last_cnt_', 'last_cnt')\
    .withColumnRenamed('max_cnt_', 'max_cnt')\
    .withColumnRenamed('chg_limit_', 'chg_limit')\
    .withColumnRenamed('max_limit_', 'max_limit')\
    .withColumnRenamed('blacklisted_', 'blacklisted')\
    .withColumnRenamed('reason_', 'reason')\
    .drop('count')

# insert new from pary records; set default limits
df_ins = df_new.alias('df_new')\
    .join(df_old.alias('df_old').filter(df_old.blacklisted == 'False'), 'from_party_id', 'leftanti')\
    .withColumn('last_cnt', df_new['count'])\
    .withColumn('max_cnt', df_new['count'])\
    .withColumn('chg_limit', lit(chg_limit))\
    .withColumn('max_limit', lit(max_limit))\
    .withColumn('blacklisted', (df_new['count'] > max_limit))\
    .withColumn('reason', struct('blacklisted', lit('volume'), df_new['count'], lit(max_limit), lit('new_from_party')))\
    .drop('count')

# join new to party data persisted form the stream and old to party data read from the database
# column names
# df_old: to_party_id, last_cnt, max_cnt, chg_limit, max_limit, blacklisted, reason
# df_new: to_party_id, count
# df_upd: to_party_id, last_cnt, max_cnt, chg_limit, max_limit, blacklisted, reason, count

# update old records that have matching new records; ignore parties that already have been blacklisted
df_updt = df_oldt.alias('df_oldt')\
    .filter(df_oldt.blacklisted == 'False')\
    .join(df_newt.alias('df_newt'), 'to_party_id', 'inner')\
    .withColumn('last_cnt_', df_newt['count'])\
    .withColumn('max_cnt_', greatest(df_oldt['max_cnt'], df_newt['count']))\
    .withColumn('chg_limit_', df_oldt['chg_limit'])\
    .withColumn('max_limit_', df_oldt['max_limit'])\
    .withColumn('blacklisted_', (greatest(df_oldt['max_cnt'], df_newt['count']) > df_oldt['max_limit']) | ((df_newt['count']/df_oldt['last_cnt']) > df_oldt['chg_limit']))\
    .withColumn('reason_', struct('blacklisted_', lit('volume'), df_oldt['max_cnt'], df_newt['count'], df_oldt['max_limit'], lit('acceleration'), df_newt['count'], df_oldt['last_cnt'], df_oldt['chg_limit']))\
    .drop('last_cnt')\
    .drop('max_cnt')\
    .drop('chg_limit')\
    .drop('max_limit')\
    .drop('blacklisted')\
    .drop('reason')\
    .withColumnRenamed('last_cnt_', 'last_cnt')\
    .withColumnRenamed('max_cnt_', 'max_cnt')\
    .withColumnRenamed('chg_limit_', 'chg_limit')\
    .withColumnRenamed('max_limit_', 'max_limit')\
    .withColumnRenamed('blacklisted_', 'blacklisted')\
    .withColumnRenamed('reason_', 'reason')\
    .drop('count')

# insert new to pary records; set default limits
df_inst = df_newt.alias('df_newt')\
    .join(df_oldt.alias('df_oldt').filter(df_oldt.blacklisted == 'False'), 'to_party_id', 'leftanti')\
    .withColumn('last_cnt', df_newt['count'])\
    .withColumn('max_cnt', df_newt['count'])\
    .withColumn('chg_limit', lit(chg_limit))\
    .withColumn('max_limit', lit(max_limit))\
    .withColumn('blacklisted', (df_newt['count'] > max_limit))\
    .withColumn('reason', struct('blacklisted', lit('volume'), df_newt['count'], lit(max_limit), lit('new_to_party')))\
    .drop('count')

# update party table
df_p.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="party", keyspace="p2p")\
    .save()

# update from_party_stats table: apply updates and inserts separatelly

df_upd.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="from_party_stats", keyspace="p2p")\
    .save()

df_ins.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="from_party_stats", keyspace="p2p")\
    .save()

# update to_party_stats table: apply updates and inserts separatelly

df_updt.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="to_party_stats", keyspace="p2p")\
    .save()


df_inst.write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="to_party_stats", keyspace="p2p")\
    .save()

# save batch processing end time
process_end_dt = datetime.datetime.now()

# mark batch as completed
session = cluster.connect('p2p')
session.execute("""insert into ops_batch(id, batch_start_dt, batch_end_dt, process_start_dt, process_end_dt) values (uuid(), %s, %s, %s, %s)""", (start_dt, end_dt, process_start_dt, process_end_dt))
