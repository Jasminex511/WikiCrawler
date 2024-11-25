import scrapy
import re
import datetime

from scrapyProject.items import WikiItem
from pymongo.mongo_client import MongoClient
from itemadapter import ItemAdapter
from urllib.parse import urlparse


class WikiSpider(scrapy.Spider):
    name = "wiki"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org/wiki/Main_Page"]
    max_depth = 100

    def __init__(self, *args, mongo_uri, mongo_db, mongo_collection, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = None
        self.db = None
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_collection = mongo_collection

    @classmethod
    def from_crawler(cls, crawler):
        spider = cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "items"),
            mongo_collection=crawler.settings.get("MONGO_COLLECTION", "wiki_items"),
        )
        spider._set_crawler(crawler)
        return spider

    def open_connection(self):
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_connection(self):
        if self.client:
            self.client.close()

    def is_in_db(self, url):
        return self.db[self.mongo_collection].find_one({"url": url}) is not None

    def add_url(self, url):
        item = WikiItem(
            url=url,
            title=None,
            photo_url=None,
            timestamp=datetime.datetime.utcnow(),
            status="incomplete"
        )
        self.db[self.mongo_collection].insert_one(ItemAdapter(item).asdict())

    def get_next_url(self, query):
        doc = self.db[self.mongo_collection].find_one(query, sort=[("timestamp", 1)])
        return doc["url"] if doc else None

    def mark_crawling(self, url):
        self.db[self.mongo_collection].update_one({"url": url}, {"$set": {"status": "crawling"}})

    def add_content(self, url, title, photo_url):
        self.db[self.mongo_collection].update_one({"url": url}, {"$set": {"title": title, "photo_url":photo_url, "status": "complete"}})

    def start_requests(self):
        self.open_connection()
        query = {"status": "crawling"}
        next_url = self.get_next_url(query)
        if not next_url:
            self.logger.info("No in progress crawling records from last time")
            query = {"status": "incomplete"}
            next_url = self.get_next_url(query)
        next_url = self.get_next_url(query)
        if next_url:
            yield scrapy.Request(url=next_url, callback=self.parse)
        else:
            for url in self.start_urls:
                if not self.is_in_db(url):
                    self.add_url(url)
                yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response, depth=0):
        if depth > self.max_depth:
            return

        self.mark_crawling(response.url)

        for link in response.css("a::attr(href)").getall()[:100]:
            full_url = response.urljoin(link)
            domain = urlparse(full_url).netloc
            if re.match(r".*/wiki/([A-Z][a-z]+_[A-Z][a-z]+)$", link) and domain in self.allowed_domains and not self.is_in_db(full_url):
                self.add_url(full_url)

        yield scrapy.Request(url=response.url, callback=self.parse_article, cb_kwargs={'depth': depth + 1}, dont_filter=True)

    def parse_article(self, response, depth):
        self.logger.info(f"parse_article called for URL: {response.url}")
        title = response.css("span.mw-page-title-main::text").get()
        photo_url = response.css("img.mw-file-element::attr(src)").get()

        # folder_name = "raw_html"
        # os.makedirs(folder_name, exist_ok=True)
        # file_name = f"{title if title else 'untitled'}.html"
        # file_path = os.path.join(folder_name, file_name)
        # with open(file_path, "w", encoding="utf-8") as f:
        #     f.write(response.text)

        self.add_content(response.url, title, photo_url)
        query = {"status": "incomplete"}
        next_url = self.get_next_url(query)
        self.logger.info(f"the next url is: {next_url}")
        if next_url and depth < self.max_depth:
            yield scrapy.Request(url=next_url, callback=self.parse, cb_kwargs={'depth': depth + 1})