# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from dataclasses import dataclass
import datetime


@dataclass
class WikiItem:
    url: str
    title: str
    photo_url: int
    status: str
    timestamp: datetime

class ScrapyprojectItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass
