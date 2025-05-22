import os

from dotenv import load_dotenv

load_dotenv()

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BROKER'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_USERNAME'),
    'sasl.password': os.getenv('KAFKA_PASSWORD'),
}

MONGO_URI = f"mongodb+srv://{os.getenv('MONGODB_USERNAME')}:{os.getenv('MONGODB_PASSWORD')}@wikidb.8zvpa.mongodb.net/?retryWrites=true&w=majority&appName=WikiDB"

ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "profile_index"