from elasticsearch import Elasticsearch
from config.settings import ELASTICSEARCH_URL, INDEX_NAME

def recreate_index():
    es = Elasticsearch(ELASTICSEARCH_URL)

    if es.indices.exists(index=INDEX_NAME):
        print(f"Deleting existing index: {INDEX_NAME}")
        es.indices.delete(index=INDEX_NAME)

    print(f"Creating new index: {INDEX_NAME}")
    es.indices.create(
        index=INDEX_NAME,
        body={
            "mappings": {
                "properties": {
                    "name": {"type": "text"},
                    "title": {"type": "text"},
                    "keywords": {"type": "text"},
                    "url": {"type": "keyword"},
                    "spouse": {"type": "text"},
                    "birthdate": {"type": "date"},
                    "nationality": {"type": "keyword"}
                }
            }
        }
    )
    print("✅ Index reset complete.")

if __name__ == "__main__":
    recreate_index()
