# coding=utf-8
'''
viki - 2016.12.24
scrapy: 1.0.3
python: 2.7.12
system: mac10,windows8
output coding: utf-8
''' 

import scrapy
from yingjiesheng.items import YingjieshengItem
import re

class YingjieshengSpider(scrapy.Spider):
    name = "yingjiesheng"

    DOWNLOAD_DELAY = 10
    COOKIES_ENABLED=False

    def mkString(self,x,s):
        res = x[0]
        for i in range(1,len(x)):
            res = res + s + x[i]
        return(res)


    def start_requests(self):
    	page_num=20
        keyword='数据挖掘'
    	for page in range(0,page_num):
    		url= 'http://s.yingjiesheng.com/result.jsp?keyword=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&start='+str(page)+'0&period=0&sort=score&jobtype=0'
    		print '######################  第%d页  ###################:%s' % (page+1,url)
    		yield scrapy.Request(url=url, callback=self.parse_page)
    	# url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E4%B8%8A%E6%B5%B7&kw=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&sm=0&kt=3&sg=f1a32f3f4e99491489067994c5444c74&p=1'
    	# yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
    	urls = response.xpath('//h3[@class="title"]/a/@href').extract()
        for (i,url) in enumerate(urls):
            # if (i>5): break
            item = YingjieshengItem()
            item['job_url'] = url
            yield scrapy.Request(url=url, callback=self.parse_content, meta = item)
            
    def parse_content(self,response):
        item=response.meta
        # 岗位名称
        job_name = response.xpath('//div[@class="main mleft"]/h1/text()').extract_first()
        if job_name is None: job_name = response.xpath('//div[@class="mleft"]/h1/text()').extract_first()
        item['job_name'] = job_name; print(item['job_name'])
        
        # 岗位信息
        info = response.xpath('//div[@class="info clearfix"]/ol/li/u/text()').extract()
        # @ 工作性质
        item['job_attr'] = info[2]; print(item['job_attr'])
        # @ 发布时间
        item['date'] = info[0]; print(item['date'])
        # @ 工作地点
        item['job_place'] = info[1]

        # 具体要求
        dr = re.compile(r'<[^>]+>',re.S)
        dr2 = re.compile(r'<br>', re.S)
        job_info = response.xpath('//div[@class="jobIntro"]').extract_first()
        job_info = job_info.replace('\r\n','')
        job_info = job_info.replace('\t','')
        job_info = dr2.sub('\n',job_info)
        item['job_info'] = dr.sub('',job_info)

        yield item



