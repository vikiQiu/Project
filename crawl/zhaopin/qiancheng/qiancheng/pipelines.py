# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import csv

class QianchengPipeline(object):

	#def open_spider(self,spider):
	#	print '#######################################'
	#	self.f=open('data.csv','wb')
	#	self.ff=csv.writer(self.f)
#
	#def close_spider(self,spider):
	#	self.f.close()

    def process_item(self, item, spider):
        return item
