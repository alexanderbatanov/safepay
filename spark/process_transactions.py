# process_transactions - SafePay spark streaming job

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
from datetime import datetime
import uuid

# spark
sc = SparkContext(appName="transactions1")
sc.setLogLevel('ERROR')
ssc = StreamingContext(sc, 2)

def processRDD(rdd):
 
    if rdd.isEmpty():
        return

    rdd2 = rdd.map(lambda x: str(uuid.uuid4()) + ";" + x)
    rdd3 = rdd2.map(lambda x: x.split(";"))

    rdd3.foreachPartition(processRDDPartition)      

def processRDDPartition(rddPartition):

    # Cassandra
    cluster = Cluster(['0.0.0.0', '0.0.0.0', '0.0.0.0'])
    session = cluster.connect('p2p')
    
    # prepare statements only once
    prep_stmt_f = session.prepare("select blacklisted from from_party_stats where from_party_id = ?")
    prep_stmt_t = session.prepare("select blacklisted from to_party_stats where to_party_id = ?")
    prep_stmt_tx = session.prepare("""insert into tx(tx_id,\
        tx_dt,\
        from_party_id,\
        from_party_name,\
        to_party_id,\
        to_party_name,\
        denied,\
        reason)\
        values (?, ?, ?, ?, ?, ?, ?, ?) using ttl 3600""")
    prep_stmt_c = session.prepare("""update ops_tx_cnt set cnt = cnt + ? where id = ?""")
    
    approved_cnt = 0
    denied_cnt = 0
    unknown_cnt = 0
    
    for r in rddPartition:
    
        # assign transaction id
        tx_id = uuid.uuid4()

        # get from party id and two party id from rdd
        from_party_id = r[4]
        to_party_id = r[6]

        # lookup from party party blacklisted flag in the database
        param_f = []
        param_f.append(from_party_id)
        rowsf = session.execute(prep_stmt_f, param_f)
        
        # set from party's blacklisted and unknown flags
        if rowsf:
            from_blacklisted = rowsf[0].blacklisted
            from_unknown = False
        else:
            from_blacklisted = False
            from_unknown = True

        # lookup to party blacklisted flag in the database
        param_t = []
        param_t.append(to_party_id)
        rowst = session.execute(prep_stmt_t, param_t)
        
        # set to party's blacklisted and unknown flags
        if rowst:
            to_blacklisted = rowst[0].blacklisted
            to_unknown = False
        else:
            to_blacklisted = False
            to_unknown = True

        # unknown party check
        unknown = from_unknown or to_unknown      

        if unknown:
            unknown_cnt += 1
            reason = 'first_seen'

        # approved or denied decision and reason
        denied = from_blacklisted or to_blacklisted
        
        if denied:
            denied_cnt += 1
        else:
            approved_cnt += 1
        
        if from_blacklisted and to_blacklisted:
            reason = 'both parties are blacklisted'
        elif from_blacklisted:
            reason = 'from party is blacklisted'
        elif to_blacklisted:
            reason = 'to party is blacklisted'
        else:
            reason = 'neither party is blacklisted'

        # save transaction to the database
        param_tx = []
        param_tx.append(tx_id)
        param_tx.append(datetime.strptime(r[3], "%Y-%m-%dT%H:%M:%SZ")) # convert source datetime string representation
        param_tx.append(r[4])
        param_tx.append(r[5])
        param_tx.append(r[6])
        param_tx.append(r[7])
        param_tx.append(denied)
        param_tx.append(reason)
        bound_stmt_tx = prep_stmt_tx.bind(param_tx)
        session.execute(bound_stmt_tx)
        
    # update counts in the dataabse at the end of processing RDDPartition
    
    param_c = []
    param_c.append(unknown_cnt)
    param_c.append('unknown')
    session.execute(prep_stmt_c, param_c)

    param_c = []
    param_c.append(denied_cnt)
    param_c.append('denied')
    session.execute(prep_stmt_c, param_c)
  
    param_c = []
    param_c.append(approved_cnt)
    param_c.append('approved')
    session.execute(prep_stmt_c, param_c)


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