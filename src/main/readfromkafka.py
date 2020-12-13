import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


server = "localhost:9092"
topic = "sample"
key_list = ["data1", "data2"]
schema_1 = StructType([StructField("tag", StringType(), True), StructField("name", StringType(), True)])
schema_2 = StructType(
    [StructField("device_id", StringType(), True), StructField("device_name", StringType(), True)])
json_schema_map = {}
json_schema_map.update({key_list[0]: schema_1})
json_schema_map.update({key_list[1]: schema_2})


def create_spark_session():
    """
    returns spark session
    :return:
    """
    spark = SparkSession.builder.appName("Kafka multiple schema") \
        .master("local[*]") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4") \
        .getOrCreate()
    return spark


def read_from_kafka(spark):
    """

    :param spark: sparksession
    :return: returns dataframe read from Kafka topic
    """
    kafkadf = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", server) \
        .option("subscribe", topic) \
        .load()
    return kafkadf


def get_schema(input_key):
    """
    Receives input key and returns json schema
    :param input_key: Kafka topic value to get the schema
    :return:  json schema for that key
    """
    return json_schema_map.get(input_key)


def get_json_df(kafka_df):
    """
    dataframe read from kafka topic
    :param kafka_df:
    :return:
    """
    for key in key_list:
        json_df = kafka_df.select("value").withColumn("value", from_json(col("value").cast(StringType()),
                                                                         get_schema(key)))
        final_df = json_df.select("value.*")
        test_df = final_df.na.drop()
        test_df.withColumn("key", lit(key)).writeStream.format("console").start()


def main():
    spark = create_spark_session()
    kafka_df = read_from_kafka()
    get_json_df(kafka_df)
    spark.streams.awaitAnyTermination()


if __name__ == '__main__':
    main()
