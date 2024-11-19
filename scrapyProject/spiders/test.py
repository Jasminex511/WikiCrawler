import scrapy
import re
from scrapyProject.items import WikiItem
from scrapyProject.spiders.get_visited import ReadMongo


class WikiSpider(scrapy.Spider):
    name = "wiki"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org/wiki/Main_Page"]
    max_depth = 3

    def __init__(self, *args, mongo_uri, mongo_db, **kwargs):
        super().__init__(*args, **kwargs)
        self.mongo_handler = ReadMongo(
            mongo_uri=mongo_uri,
            mongo_db=mongo_db,
            collection_name="wiki_items"
        )
        self.visited = set()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get("MONGO_URI"),
            mongo_db=crawler.settings.get("MONGO_DATABASE", "items"),
        )

    def open_spider(self, spider):
        self.mongo_handler.open_connection()
        self.visited = self.mongo_handler.get_visited_urls()

    def close_spider(self, spider):
        self.mongo_handler.close_connection()

    def parse(self, response, depth=0):
        if depth > self.max_depth:
            return

        for link in response.css("a::attr(href)").getall()[:100]:
            full_url = response.urljoin(link)
            if re.match(r".*/wiki/([A-Z][a-z]+_[A-Z][a-z]+)$", link) and full_url not in self.visited:
                self.visited.add(full_url)
                yield scrapy.Request(url=full_url, callback=self.parse_article, cb_kwargs={'depth': depth + 1})

    def parse_article(self, response, depth):
        title = response.css("span.mw-page-title-main::text").get()
        photo_url = response.css("img.mw-file-element::attr(src)").get()
        yield WikiItem(
            url=response.url,
            title=title,
            photo_url=response.urljoin(photo_url) if photo_url else None
        )

        yield from self.parse(response, depth=depth)
