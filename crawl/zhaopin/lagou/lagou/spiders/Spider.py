# coding=utf-8
'''
viki - 2016.12.21
scrapy: 1.0.3
python: 2.7.12
system: mac10,windows8
output coding: utf-8
''' 

import scrapy

class LagouSpider(scrapy.Spider):
	name = "lagou"

	start_urls = (
		'https://www.lagou.com/zhaopin/',
	)

	def parse(self, response):
		print response.body