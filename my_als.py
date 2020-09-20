from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Row
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

kafka_value_schema = StructType([
    StructField("user_name",  StringType(), True),
    StructField("whiskeyId",  IntegerType(), True),   
])


def parse_data_from_kafka_message(sdf, schema):
  from pyspark.sql.functions import split
  assert sdf.isStreaming == True, "DataFrame doesn't receive streaming data"
  col = split(sdf['value'], ',')
  #now expand col to multiple top-level columns
  for idx, field in enumerate(schema): 
      sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))      
  return sdf.select([field.name for field in schema])

def whiskey_als(df):
    model = ALSModel.load("hdfs://master/ALSModel/")
    predict = model.recommendForItemSubset(df, 1)
    df_user = predict.select(
        predict.whiskeyId, 
        predict.recommendations[0].userId.alias("userId"),
    )
    
    df_whiskey= model.recommendForUserSubset(df_user, 5)
    result_df = df_user.join(df_whiskey, on=['userId'], how='left')
    result_df = result_df.join(df, on=['whiskeyId'], how='left')
    result_df = result_df.select("user_name", "whiskeyId", "recommendations")
    return result_df

if __name__ == "__main__":
        
    sc = SparkContext()
    
    spark = SparkSession.builder.appName('Lin').getOrCreate()
    
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers",  "10.120.26.247:9092") \
        .option("subscribe", "whiskey") \
        .option("checkpointLocation", "/tmp/checkpoint") \
        .load()\
        .withColumn("value",(col("value").cast("string")))\
        .selectExpr("CAST(value as STRING)")

    spark.sparkContext.setLogLevel("ERROR")
    df = parse_data_from_kafka_message(df, kafka_value_schema)
    df = df.select("whiskeyId")
    df = ALSModel.load("hdfs://master/ALSModel/").recommendForItemSubset(df, 1)
    
    # result_df = whiskey_als(df)
    
    query = df\
                .writeStream \
                .outputMode("complete") \
                .format("console") \
                .trigger(processingTime='5 seconds') \
                .start()
    query.awaitTermination()