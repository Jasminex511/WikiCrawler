import json
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

from config.settings import MONGO_URI
from consumer.base_consumer import BaseConsumer


class MongoDBConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(group_id="mongodb-consumer")

        self.client = MongoClient(MONGO_URI, server_api=ServerApi('1'))

        try:
            self.client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(f"MongoDB connection failed: {e}")

        self.collection = self.client["wiki_profile"]["wiki_profile"]

    def process_messages(self):
        try:
            for message in self.consume_messages("wiki-profile"):
                try:
                    record = json.loads(message)
                    self.collection.insert_one(record)
                    print(f"[✓] Inserted: {record.get('title', 'unknown')}")
                except Exception as e:
                    print(f"[MongoDB Insert Error] {e}")
        except KeyboardInterrupt:
            print("Shutting down MongoDB consumer.")
        finally:
            self.close()
            self.client.close()


if __name__ == "__main__":
    consumer = MongoDBConsumer()
    consumer.process_messages()