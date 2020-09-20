from confluent_kafka import Producer
import sys
import pandas as pd
import time
from datetime import datetime,timedelta


def error_cb(err):
    print('Error: %s' % err)



if __name__ == '__main__':

    props = {

        'bootstrap.servers': '10.120.26.247:9092',
        'error_cb': error_cb
    }

    producer = Producer(props)

    topicName = 'whiskey'
    msgCounter = 0    
    try:
        while True:
                import random
                name = random.choice(["Eddie", "Arthur", "David", "Nan", "Rudy", "Jordan"])
                i = random.choice([i for i in range(9378)])
                c=f"{name},{str(i)}"
                # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
                producer.produce(topicName, value=bytes(c,encoding="utf8"))

                # producer.flush()

                print('Send ' + str(msgCounter) + ' messages to Kafka  message--> ' + c)
                time.sleep(1)
                msgCounter += 1
    except BufferError as e:

        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)

    producer.flush()

