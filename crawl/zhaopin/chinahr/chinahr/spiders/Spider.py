# coding=utf-8
'''
viki - 2016.12.21
scrapy: 1.0.3
python: 2.7.12
system: mac10,windows8
output coding: utf-8
''' 

import scrapy
from chinahr.items import ChinahrItem
import re

class ChinahrSpider(scrapy.Spider):
    name = "chinahr"

    DOWNLOAD_DELAY = 1
    COOKIES_ENABLED=False

    def mkString(self,x,s):
        res = x[0]
        for i in range(1,len(x)):
            res = res + s + x[i]
        return(res)


    def start_requests(self):
    	page_num=1
        keyword='大数据'
    	for page in range(0,page_num):
    		url= 'http://www.chinahr.com/sou/?orderField=relate&keyword=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&page='+str(page+1)
    		print '######################  第%d页  ###################:%s' % (page+1,url)
    		yield scrapy.Request(url=url, callback=self.parse_page)
    	# url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E4%B8%8A%E6%B5%B7&kw=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&sm=0&kt=3&sg=f1a32f3f4e99491489067994c5444c74&p=1'
    	# yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
    	urls = response.xpath('//span[@class="e1"]/a/@href').extract()
        for (i,url) in enumerate(urls):
            if (i>5): break
            item = ChinahrItem()
            item['job_url'] = url
            yield scrapy.Request(url=url, callback=self.parse_content, meta = item)
            
    def parse_content(self,response):
        item=response.meta
        # 岗位名称
        item['job_name'] = response.xpath('//span[@class="job_name"]/text()').extract_first(); print(item['job_name'])
        
        # 岗位信息
        info = response.xpath('//div[@class="job_require"]/span/text()').extract()
        # @ 最低学历
        item['edu'] = info[3]; print (item['edu'])
        # @ 工作经验
        item['exp'] = response.xpath('//span[@class="job_exp"]/text()').extract_first()
        # @ 工作性质
        item['job_attr'] = info[2]; print(item['job_attr'])
        # @ 发布时间
        item['date'] = response.xpath('//p[@class="updatetime"]/text()').extract_first()
        # @ 工作地点
        item['job_place'] = response.xpath('//span[@class="job_loc"]/text()').extract_first()
        # @ 工资
        item['salary'] = response.xpath('//span[@class="job_price"]/text()').extract_first()
        # @ 工作标签
        job_tags = response.xpath('//div[@class="job_fit_tags"]/ul/li/text()').extract()
        item['job_tags'] = self.mkString(job_tags,"|")

        dr = re.compile(r'<[^>]+>',re.S)
        # 公司信息
        infos = response.xpath('//div[@class="job-company jrpadding"]/table/tbody/tr')
        # @ 公司名称
        item['job_enterprise'] = response.xpath('//h4/a/text()').extract_first()
        for info in infos:
            key = info.xpath('./td[@class="e1"]/text()').extract_first()
            if(re.sub(' ', '', key) == unicode('规模',"utf-8")):
                # @ 公司规模
                item['enterprise_scale'] = info.xpath('./td/text()').extract()[1]
            elif(re.sub(' ', '', key) == unicode('性质',"utf-8")):
                # @ 公司性质
                item['enterprise_attr'] = info.xpath('./td/text()').extract()[1]
        # @ 公司行业
        item['enterprise_industry'] = response.xpath('//div[@class="compny_tag"]/span[@class="job_loc"]/text()').extract_first()
        # @ 具体要求
        job_info = response.xpath('//div[@class="job_intro_info"]').extract_first()
        item['job_info'] = dr.sub('',job_info)

        yield item



