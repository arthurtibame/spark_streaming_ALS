from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import Row
from pyspark.ml.recommendation import ALSModel
from confluent_kafka import Producer
from pyspark.ml.feature import IndexToString
import json 
from datetime import datetime
def kafka_error_cb(err):
    print('Error: %s' % err)

def index2whiskey():
    pass

def handler(message):
    topicName = 'whiskey_out'
    props = {

        'bootstrap.servers': '10.120.26.247:9092',
        'error_cb': kafka_error_cb
    }
    producer = Producer(props)
    records = message.collect()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n======================={now}=========================\n")
    for record in records:
        user_name = record.user_name        
        whiskey1 = record.whiskey1
        whiskey2 = record.whiskey2
        whiskey3 = record.whiskey3
        whiskey4 = record.whiskey4
        whiskey5 = record.whiskey5
        message = f"{user_name},{whiskey1},{whiskey2},{whiskey3},{whiskey4},{whiskey5},"
        producer.produce(topicName, value=bytes(str(message),encoding="utf8"))

        print(message)

    producer.flush()

def preprocessing(line):
    line = line[1].split(",")
    return (line[0], int(line[1]))

def my_transform(rdd):    
    with open ("./index2whiskey1.json", mode="r", encoding="utf-8") as f:
        whiskey_list = list(json.loads(f.read()).values())
    model = ALSModel.load("hdfs://master/ALSModel1/")
    spark = SparkSession.builder.appName('sql coming~').getOrCreate()
    whiskey = rdd.map(lambda x: Row(whiskeyId=int(x[1]), user_name=x[0]))
    whiskey_df = spark.createDataFrame(whiskey)
    predict = model.recommendForItemSubset(whiskey_df, 1)    
    df_user = predict.select(
        predict.whiskeyId, 
        predict.recommendations[0].userId.alias("userId"),
    )
    
    df_whiskey= model.recommendForUserSubset(df_user, 5)
    result_df = df_user.join(df_whiskey, on=['userId'], how='left')
    result_df = result_df.join(whiskey_df, on=['whiskeyId'], how='left')
    result_df = result_df.select("user_name", 
                    result_df["recommendations"][0].whiskeyId.alias("whiskeyId1"), \
                    result_df["recommendations"][1].whiskeyId.alias("whiskeyId2"), \
                    result_df["recommendations"][2].whiskeyId.alias("whiskeyId3"), \
                    result_df["recommendations"][3].whiskeyId.alias("whiskeyId4"), \
                    result_df["recommendations"][4].whiskeyId.alias("whiskeyId5") \
                    )                           
    whiskeyId1converter = IndexToString(inputCol="whiskeyId1", outputCol="whiskey1", labels=whiskey_list)
    whiskeyId2converter = IndexToString(inputCol="whiskeyId2", outputCol="whiskey2", labels=whiskey_list)
    whiskeyId3converter = IndexToString(inputCol="whiskeyId3", outputCol="whiskey3", labels=whiskey_list)
    whiskeyId4converter = IndexToString(inputCol="whiskeyId4", outputCol="whiskey4", labels=whiskey_list)
    whiskeyId5converter = IndexToString(inputCol="whiskeyId5", outputCol="whiskey5", labels=whiskey_list)
    
    result_df = whiskeyId1converter.transform(result_df)
    result_df = whiskeyId2converter.transform(result_df)
    result_df = whiskeyId3converter.transform(result_df)
    result_df = whiskeyId4converter.transform(result_df)
    result_df = whiskeyId5converter.transform(result_df)

    return result_df.rdd

if __name__ == "__main__":
    
    sc = SparkContext()
    sc.setLogLevel("WARN") # 减少shell打印日志
    
    
    ssc = StreamingContext(sc, 5) # 5秒的计算窗口
    brokers='10.120.26.247:9092'
    topic = 'whiskey'
    # 使用streaming使用直连模式消费kafka 
    kafka_streaming_rdd = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines_rdd = kafka_streaming_rdd.map(preprocessing)
    lines_rdd = lines_rdd.transform(my_transform)
    lines_rdd.foreachRDD(handler)
    # lines_rdd.pprint()
    ssc.start()
    ssc.awaitTermination()
