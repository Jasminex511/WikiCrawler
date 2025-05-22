from flask import Flask, request, jsonify
from flask_cors import CORS
from threading import Thread

from config.settings import INDEX_NAME
from consumer.elasticsearch_consumer import ElasticsearchConsumer

app = Flask(__name__)
CORS(app)

consumer = ElasticsearchConsumer()

@app.route("/api/v1/search", methods=["GET"])
def search():
    print("🔍 Flask route hit")

    search_query = request.args.get("search_query", "")
    skip = int(request.args.get("skip", 0))
    limit = int(request.args.get("limit", 10))

    response = consumer.es.search(
        index=INDEX_NAME,
        body={
            "query": {
                "bool": {
                    "should": [
                        {"match": {"name": search_query}},
                        {"match": {"keywords": search_query}}
                    ]
                }
            },
            "from": skip,
            "size": limit
        }
    )

    hits = response.get("hits", {}).get("hits", [])
    return jsonify({"hits": hits})

if __name__ == "__main__":
    def run_stream():
        consumer.process_messages()

    Thread(target=run_stream, daemon=True).start()
    app.run(debug=True, use_reloader=False)