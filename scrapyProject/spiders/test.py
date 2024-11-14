import scrapy
import re

from scrapyProject.items import WikiItem


class WikiSpider(scrapy.Spider):
    name = "wiki"
    allowed_domains = ["en.wikipedia.org"]
    start_urls = ["https://en.wikipedia.org/wiki/Main_Page"]
    visited = set()
    max_depth = 3

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
            title=title,
            photo_url=response.urljoin(photo_url) if photo_url else None
        )

        yield from self.parse(response, depth=depth)
