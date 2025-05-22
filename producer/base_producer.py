from confluent_kafka import Producer
import json
from config.settings import KAFKA_CONFIG


class BaseProducer:
    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)

    def send(self, topic, value: dict):
        try:
            self.producer.produce(
                topic=topic,
                value=json.dumps(value).encode("utf-8")
            )
            self.producer.poll(0)
        except Exception as e:
            print(f"[ERROR] Failed to produce message to topic {topic}: {e}")

    def flush(self):
        self.producer.flush()