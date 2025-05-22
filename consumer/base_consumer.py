from confluent_kafka import Consumer
from config.settings import KAFKA_CONFIG


class BaseConsumer:
    def __init__(self, group_id):
        self.consumer = Consumer({
            **KAFKA_CONFIG,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
        })

    def consume_messages(self, topic):
        self.consumer.subscribe([topic])
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[Kafka Error] {msg.error()}")
                continue
            yield msg.value().decode('utf-8')

    def close(self):
        self.consumer.close()