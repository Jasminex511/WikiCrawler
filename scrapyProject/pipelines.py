# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
from confluent_kafka import Producer
from scrapyProject.items import ContentItem
from scrapyProject.settings import KAFKA_CONFIG


def report(err, msg):
    if err:
        print(f"URL Kafka delivery failed: {err}")
    else:
        print(f"URL sent: {msg.topic()}")

class ContentKafkaPipeline:
    def __init__(self):
        self.producer = Producer(KAFKA_CONFIG)

    def process_item(self, item, spider):
        if isinstance(item, ContentItem):
            self.producer.produce(
                topic="wiki-content",
                value=json.dumps(dict(item)),
                callback=report
            )
            self.producer.poll(0)
        return item

    def close_spider(self, spider):
        spider.logger.info("Flushing Kafka producer from pipeline...")
        self.producer.flush()
