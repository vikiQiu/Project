# coding=utf-8
'''
viki - 2016.12.018
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
    	page_num=2
        keyword='数据挖掘'
    	# for page in range(0,1):
    	# 	url='http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E4%B8%8A%E6%B5%B7&kw=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&sm=0&kt=3&sg=f1a32f3f4e99491489067994c5444c74&p='+str(page+1)
    	# 	print '######################  第%d页  ###################:%s' % (page+1,url)
    	# 	yield scrapy.Request(url=url, callback=self.parse_page)
    	url = 'http://sou.zhaopin.com/jobs/searchresult.ashx?jl=%E4%B8%8A%E6%B5%B7&kw=%E6%95%B0%E6%8D%AE%E6%8C%96%E6%8E%98&sm=0&kt=3&sg=f1a32f3f4e99491489067994c5444c74&p=1'
    	yield scrapy.Request(url=url, callback=self.parse_page)

    def parse_page(self, response):
    	job_names = response.xpath('//a[@par]').re(r'<a.*">(.*)</a>')
    	print('Begin This Page.')
    	for name in names:
    		print (name)
    	print(len(names))
    	# bodys=response.css('div.el')
    	# for index,body in enumerate(bodys):
    	# 	name = body.css('')
    	    # url=body.css('p.t1 span a::attr(href)').extract_first()
    	    # if url is None:
    	    # 	continue
    	    # #print '＃＃＃＃第%d个岗位########## new new new' % index
    	    # item=QianchengItem()
    	    # item['job_name']=body.css('p.t1 span a::attr(title)').extract_first()
    	    # item['job_url']=url
    	    # item['job_enterprise']=body.css('span.t2 a::attr(title)').extract_first()
    	    # item['job_place']=body.css('span.t3::text').extract_first()
    	    # if body.css('span.t4::text'):
    	    # 	item['salary']=body.css('span.t4::text').extract_first()
    	    # else:
    	    # 	item['salary']='Null'
    	    # item['date']=body.css('span.t5::text').extract_first()
         #    if index > 50: # 方便调试放的if 可以去除
    	    #     yield scrapy.Request(url=url,callback=self.parse_content,meta=item)
         #        item['edu']=response.css('em.i2::text').extract_first()
            
    # def parse_content(self,response):
    #     item=response.meta
    #     temp=response.css('span.sp4')
    #     # @ education
    #     # edu=temp.re(r'<em class="i2"></em>\s*(.*)</span>')
    #     # item['edu']=edu if len(edu)==1 else 'Null'
    #     # 下面不用判断null
    #     item['edu']=response.xpath('//em[@class="i2"]/parent::span/text()').extract_first()
    #     # @ 招聘人数
    #     # num=temp.re(r'<em class="i3"></em>\s*(.*)</span>')
    #     # item['job_num']=num if len(num)==1 else 'Null'
    #     item['job_num']=response.xpath('//em[@class="i3"]/parent::span/text()').extract_first()
    #     # @ 工作经验
    #     # exp=temp.re(r'<em class="i1"></em>\s*(.*)</span>')
    #     # item['job_exp']=exp if len(exp)==1 else 'Null'
    #     item['job_exp']=response.xpath('//em[@class="i1"]/parent::span/text()').extract_first()
    #     # @ 具体信息
    #     temp=response.xpath('//div[@class="bmsg job_msg inbox"]/text()').extract()
    #     info=''
    #     for i in temp:
    #         info=info+i.strip()
    #     item['job_info']=info
    #     # @ 公司信息
    #     temp=response.xpath('//p[@class="msg ltype"]/text()').extract_first().split()
    #     item['enterprise_attr']=temp[0]
    #     item['enterprise_scale']=temp[2]
    #     item['enterprise_industry']=temp[4]

    #     yield item

