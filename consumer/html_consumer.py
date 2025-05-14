from pyspark.sql.functions import udf, col, from_json, to_json, struct
from pyspark.sql.types import StringType, ArrayType, StructField, StructType

from config.settings import KAFKA_CONFIG
from consumer.base_consumer import BaseConsumer
from processor.extract_json import extract_profile_info
from producer.profile_producer import ProfileProducer


class HtmlConsumer(BaseConsumer):

    def __init__(self):
        super().__init__("HtmlConsumerSpark")
        self.producer = ProfileProducer()

    def consume_message(self, topic):
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap.servers']) \
            .option("kafka.security.protocol", KAFKA_CONFIG['security.protocol']) \
            .option("kafka.sasl.mechanism", KAFKA_CONFIG['sasl.mechanisms']) \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_CONFIG["sasl.username"]}" password="{KAFKA_CONFIG["sasl.password"]}";') \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "10") \
            .load()
        return kafka_df

    def process_message(self):
        schema = ArrayType(StructType([
            StructField("url", StringType(), False),
            StructField("title", StringType(), False),
            StructField("content", StringType(), False)
        ]))

        kafka_df = self.consume_message("wiki-content")
        print("***********************************")
        profile_schema = StructType([
            StructField("name", StringType()),
            StructField("spouse", StringType()),
            StructField("birthdate", StringType()),
            StructField("nationality", StringType()),
            StructField("keywords", StringType())
        ])
        extract_profile_info_udf = udf(extract_profile_info, profile_schema)

        json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
        parsed_df = json_df.withColumn("data", from_json(col("json_str"), schema))
        exploded_df = parsed_df.selectExpr("explode(data) as entry")
        result_df = exploded_df.withColumn("profile", extract_profile_info_udf(col("entry.content")))
        result_df = result_df \
            .withColumn("name", col("profile").getItem("name")) \
            .withColumn("spouse", col("profile").getItem("spouse")) \
            .withColumn("birthdate", col("profile").getItem("birthdate")) \
            .withColumn("nationality", col("profile").getItem("nationality")) \
            .withColumn("keywords", col("profile").getItem("keywords"))
        output_df = result_df.select(
            to_json(struct(
                col("entry.url").alias("url"),
                col("entry.title").alias("title"),
                col("name"),
                col("spouse"),
                col("birthdate"),
                col("nationality"),
                col("keywords")
            )).alias("value")
        )
        query = self.producer.produce_message(
            output_df,
            topic="wiki-profile",
            checkpoint_location="/tmp/wiki-profile-checkpoint"
        )

        output_df.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .start()

        return query

