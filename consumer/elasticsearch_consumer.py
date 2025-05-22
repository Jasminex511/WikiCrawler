import json
from elasticsearch import Elasticsearch
from config.settings import ELASTICSEARCH_URL, INDEX_NAME
from consumer.base_consumer import BaseConsumer


class ElasticsearchConsumer(BaseConsumer):
    def __init__(self):
        super().__init__(group_id="elasticsearch-consumer")
        self.es = Elasticsearch(ELASTICSEARCH_URL)

        if not self.es.indices.exists(index=INDEX_NAME):
            self._create_index(INDEX_NAME)

    def _create_index(self, index_name):
        self.es.indices.delete(index=index_name, ignore_unavailable=True)
        self.es.indices.create(index=index_name, body={
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "title": {"type": "text"},
                    "name": {"type": "text"},
                    "spouse": {"type": "text"},
                    "birthdate": {"type": "keyword"},
                    "nationality": {"type": "text"},
                    "keywords": {"type": "text"}
                }
            }
        })

    def _insert_documents(self, index_name, documents):
        if not documents:
            return

        operations = []
        for doc in documents:
            doc_id = doc.pop("_id", None) or doc.get("url")
            op = {"index": {"_index": index_name}}
            if doc_id:
                op["index"]["_id"] = doc_id
            operations.append(op)
            operations.append(doc)

        print(f"[Elasticsearch] Inserting {len(documents)} documents...")
        response = self.es.bulk(body=operations)

        if response.get("errors"):
            print("❌ Bulk insert had errors!")
            for item in response["items"]:
                if "error" in item["index"]:
                    print(item["index"]["error"])
        else:
            print("✅ Bulk insert succeeded.")

    def process_messages(self):
        batch = []
        try:
            for message in self.consume_messages("wiki-profile"):
                try:
                    doc = json.loads(message)
                    doc["_id"] = doc.get("url", "")
                    batch.append(doc)
                except Exception as e:
                    print(f"[❌ JSON Parse Error] {e}")

                if len(batch) >= 10:
                    self._insert_documents(INDEX_NAME, batch)
                    batch.clear()

        except KeyboardInterrupt:
            print("Shutting down Elasticsearch consumer.")

        finally:
            if batch:
                self._insert_documents(INDEX_NAME, batch)
            self.close()


if __name__ == "__main__":
    consumer = ElasticsearchConsumer()
    consumer.process_messages()