from consumer.html_consumer import HtmlConsumer

if __name__ == "__main__":
    consumer = HtmlConsumer()
    query = consumer.process_message()
    print(query)
    query.awaitTermination()

