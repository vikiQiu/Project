# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

#import scrapy
from scrapy.item import Item, Field


class ZhilianItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    # 岗位名称
    job_name=Field()
    # 对应链接
    job_url=Field()
    # 公司名称
    job_enterprise=Field()
    # 工作地点
    job_place=Field()
    # 工资
    salary=Field()
    # date
    date=Field()
    # 教育情况
    edu=Field()
    # 招聘人数
    job_num=Field()
    # 工作经验
    job_exp=Field()
    # 具体要求和信息
    job_info=Field()
    # 公司性质
    enterprise_attr=Field()
    # 公司规模
    enterprise_scale=Field()
    # 行业
    enterprise_industry=Field()
    # 工作性质
    job_attr = Field()
    pass
