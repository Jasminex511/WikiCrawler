from config.settings import KAFKA_CONFIG
from producer.base_producer import BaseProducer


class ProfileProducer(BaseProducer):

    def __init__(self):
        super().__init__("PersonProducerSpark")

    def produce_message(self, output_df, topic, checkpoint_location):
        return output_df.writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_CONFIG['bootstrap.servers']) \
            .option("kafka.security.protocol", KAFKA_CONFIG['security.protocol']) \
            .option("kafka.sasl.mechanism", KAFKA_CONFIG['sasl.mechanisms']) \
            .option("kafka.sasl.jaas.config",
                    f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_CONFIG["sasl.username"]}" password="{KAFKA_CONFIG["sasl.password"]}";') \
            .option("topic", topic) \
            .option("checkpointLocation", checkpoint_location) \
            .start()