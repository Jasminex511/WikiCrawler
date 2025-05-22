import json
from consumer.base_consumer import BaseConsumer
from processor.extract_json import extract_profile_info
from producer.profile_producer import ProfileProducer


class HtmlConsumer:
    def __init__(self):
        self.consumer = BaseConsumer(group_id="html-consumer")
        self.producer = ProfileProducer()

    def process_message(self):
        for raw_msg in self.consumer.consume_messages("wiki-content"):
            try:
                record = json.loads(raw_msg)
                url = record.get("url")
                title = record.get("title")
                content = record.get("content")
                print(f"passing {title} content to processor...")
                profile = extract_profile_info(content)

                if not profile:
                    continue

                enriched = {
                    "url": url,
                    "title": title,
                    "name": profile.get("name"),
                    "spouse": profile.get("spouse"),
                    "birthdate": profile.get("birthdate"),
                    "nationality": profile.get("nationality"),
                    "keywords": profile.get("keywords")
                }

                self.producer.send("wiki-profile", enriched)

            except Exception as e:
                print(f"[ERROR] Failed to process message: {e}")

    def shutdown(self):
        self.consumer.close()


if __name__ == "__main__":
    consumer = HtmlConsumer()
    try:
        consumer.process_message()
    except KeyboardInterrupt:
        print("Interrupted by user.")
    finally:
        consumer.shutdown()