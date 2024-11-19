from pymongo.mongo_client import MongoClient


class ReadMongo:
    def __init__(self, mongo_uri, mongo_db, collection_name):
        self.client = None
        self.db = None
        self.collection_name = collection_name
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    def open_connection(self):
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_connection(self):
        if self.client:
            self.client.close()

    def get_visited_urls(self):
        return {doc["url"] for doc in self.db[self.collection_name].find({}, {"_id": 0, "url": 1})}
