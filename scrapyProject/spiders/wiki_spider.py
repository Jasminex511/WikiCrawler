import scrapy
import re
import redis
from urllib.parse import urlparse
from confluent_kafka import Producer
from parsel import Selector

from scrapyProject.items import ContentItem
from scrapyProject.settings import KAFKA_CONFIG


class WikiSpider(scrapy.Spider):
    name = "wiki"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org/wiki/Main_Page"]
    max_depth = 100

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redis = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.producer = Producer(KAFKA_CONFIG)

    def kafka_callback(self, err, msg):
        if err:
            self.logger.error(f"Kafka error: {err}")
        else:
            self.logger.info(f"Produced to {msg.topic()} [{msg.partition()}]")

    def start_requests(self):
        for url in self.start_urls:
            if not self.redis.sismember("visited_urls", url):
                self.redis.sadd("visited_urls", url)
                yield scrapy.Request(url=url, callback=self.parse, cb_kwargs={'depth': 0})

    def parse(self, response, depth):
        if depth > self.max_depth:
            return

        for link in response.css("a::attr(href)").getall()[:100]:
            full_url = response.urljoin(link)
            domain = urlparse(full_url).netloc
            if (
                re.match(r".*/wiki/([A-Z][a-z]+_[A-Z][a-z]+)$", link)
                and domain in self.allowed_domains
                and not self.redis.sismember("visited_urls", full_url)
            ):
                self.redis.sadd("visited_urls", full_url)
                yield scrapy.Request(url=full_url, callback=self.parse, cb_kwargs={'depth': depth + 1}, dont_filter=True)

        yield scrapy.Request(url=response.url, callback=self.parse_article, cb_kwargs={'depth': depth + 1}, dont_filter=True)

    def parse_article(self, response, depth):
        title = response.css("span.mw-page-title-main::text").get()

        sel = Selector(text=response.text)
        all_text = sel.xpath('//div[@id="mw-content-text"]//text()[not(ancestor::style) and not(ancestor::script)]').getall()
        filtered = []
        for t in all_text:
            stripped = t.strip()
            if not stripped:
                continue
            if stripped == "References":
                break
            filtered.append(stripped)
        clean_text = " ".join(filtered)

        if "Born" in clean_text:
            yield ContentItem(
                url=response.url,
                title=title,
                content=clean_text
            )

    def close_spider(self, spider):
        self.logger.info("Flushing Kafka producer from spider...")
        self.producer.flush()