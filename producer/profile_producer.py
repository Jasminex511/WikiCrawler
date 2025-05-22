from producer.base_producer import BaseProducer


class ProfileProducer(BaseProducer):
    def __init__(self):
        super().__init__()

    def produce_message(self, data_rows):
        for row in data_rows:
            self.send("wiki-profile", row)
        self.flush()