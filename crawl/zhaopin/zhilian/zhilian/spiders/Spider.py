# coding=utf-8
'''
viki - 2016.12.18
scrapy: 1.0.3
python: 2.7.12
system: mac10,windows8
output coding: utf-8
''' 

import scrapy
from zhilian.items import ZhilianItem

class ZhilianSpider(scrapy.Spider):
    name = "zhilian"

    DOWNLOAD_DELAY = 1
    COOKIES_ENABLED=False

    def start_requests(self):
    	page_num=19
        keyword='数据分析'
    	for page in range(0,page_num):
    		url='http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E4%B8%8A%E6%B5%B7&kw='+keyword+'&kt=3&sg=f1a32f3f4e99491489067994c5444c74&p='+str(page+1)
    		print '######################  第%d页  ###################:%s' % (page+1,url)
    		yield scrapy.Request(url=url, callback=self.parse_page)
    	# url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E4%B8%8A%E6%B5%B7&kw=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&sm=0&kt=3&sg=f1a32f3f4e99491489067994c5444c74&p=1'
    	# yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
    	urls = response.xpath('//a[@par]/@href').extract()
        for (i,url) in enumerate(urls):
            # if (i>5): break
            item = ZhilianItem()
            item['job_url'] = url
            yield scrapy.Request(url=url, callback=self.parse_content, meta = item)
            
    def parse_content(self,response):
        item=response.meta
        # 岗位名称
        item['job_name'] = response.xpath('//h1/text()').extract_first(); print(item['job_name'])
        
        # 岗位信息
        info = response.xpath('//ul[@class="terminal-ul clearfix"]/li')
        # @ 最低学历
        item['edu'] = info[5].re(r'<strong>(.*)</strong>')[0]; print (item['edu'])
        # @ 招聘人数
        item['job_num'] = info[6].re(r'<strong>(.*)</strong>')[0]; print(item['job_num'])
        # @ 工作经验
        item['exp'] = info[4].re(r'<strong>(.*)</strong>')[0]; print(item['exp'])
        # @ 工作性质
        item['job_attr'] = info[3].re(r'<strong>(.*)</strong>')[0]; print(item['job_attr'])
        # @ 发布时间
        item['date'] = info[2].re(r'">(.*)</span>')[0]; print(item['date'])
        # @ 工作地点
        item['job_place'] = info[1].re(r'">(.*)</a>')[0]; print(item['job_place']) 
        # @ 工资
        item['salary'] = info[0].re(r'<strong>(.*)<a')[0]; print(item['salary']) 

        # 公司信息
        info2 = response.xpath('//ul[@class="terminal-ul clearfix terminal-company mt20"]/li')
        # @ 公司名称
        item['job_enterprise'] = response.xpath('//p[@class="company-name-t"]/a/text()').extract_first(); print(item['job_enterprise']  )
        # @ 公司规模
        item['enterprise_scale'] = info2[0].re(r'<strong>(.*)</strong>')[0]; print(item['enterprise_scale'])
        # @ 公司性质
        item['enterprise_attr'] = info2[1].re(r'<strong>(.*)</strong>')[0]; print(item['enterprise_attr'])
        # @ 公司行业
        item['enterprise_industry'] = info2[2].re(r'">(.*)</a>')[0]; print(item['enterprise_industry'])
        # @ 具体要求
        item['job_info'] = response.xpath('//div[@class="terminalpage-main clearfix"]/div/div').re(r'->\r\n                        (.*)\r\n')[0]

        yield item


