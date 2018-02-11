import random
import sys
import six
from datetime import datetime
from kafka.client import KafkaClient
from kafka.producer import KafkaProducer
import time

class Producer(object):

    def __init__(self, addr):
        self.producer = KafkaProducer(bootstrap_servers=addr)

    def produce_msgs(self, source_symbol, file_name):
        msg_cnt = 0

        with open(file_name) as f:
            for line in f:
                time_field = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
                str_fmt = "{};{};{}"
                message_info = str_fmt.format(source_symbol,
                                          time_field,
                                          line) #.encode('utf-8')
                print message_info
                self.producer.send('transactions1', message_info)
                print "{} transactions sent. Sending next one ...".format(msg_cnt)
		msg_cnt += 1

if __name__ == "__main__":
    args = sys.argv
    ip_addr = str(args[1])
    partition_key = str(args[2])
    filename = str(args[3])
    prod = Producer(ip_addr)
    prod.produce_msgs(partition_key, filename)
    print("Finished.")