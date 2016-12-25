# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

#import scrapy
from scrapy.item import Item, Field


class YingjieshengItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # 岗位名称
    job_name=Field()
    # 对应链接
    job_url=Field()    # 工作地点
    job_place=Field()
    # date
    date=Field()
    # 具体要求和信息
    job_info=Field()
    # 工作性质
    job_attr = Field()
    pass
