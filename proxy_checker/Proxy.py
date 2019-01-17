import redis
import json
import requests
from proxy_checker.Proxy_checker import ProxyChecker
from lxml.html import fromstring

class Proxy(object):
	#proxy_url = "http://spys.one/en/http-proxy-list/"
	proxy_url = "http://spys.one/"
	proxy_list = []

	def __init__(self):
		#payload = {'xpp':5,'xf1':0,'xf2':0,'xf4':0,'xf5':0}
		payload = {}
		r = requests.post(self.proxy_url, data=payload)
		html = r.content
		soup = fromstring(html)
		#result = soup.xpath('(//tr[@class="spy1x"]|//tr[@class="spy1xx"]) /td[1]/font[@class="spy14"]/text()')
		result = soup.xpath('//tr/td[1]/font[@class="spy14"]/text()')
		#result_scripts = soup.xpath('(//tr[@class="spy1x"]|//tr[@class="spy1xx"]) /td[1]/font[@class="spy14"]/script')
		#scripts = soup.xpath("//script")
		#script = scripts[3].text.split(';')[:-1]
		#script_dict = {}
		#for s in script:
		#	sss = s.split('=')
		#	script_dict[sss[0]] = sss[1]

		#result = result[1:]
		#for i in range(0,len(result)):
		#	ip = result[i]
		#	port = result_scripts[i].text[44:-1].split('+')
		#	port_digits = ''
		#	for p in port:
		#		p = p[1:-1].split('^')
		#		port_digits += script_dict[p[0]].split('^')[0]
		#	self.proxy_list.append('{}:{}'.format(ip,port_digits))
		for i in result:
			self.proxy_list.append(i)

	def __call__(self):
		r = redis.Redis()
		for p in self.proxy_list:
			r.rpush('proxy', bytes(json.dumps({'proxy':p,'checked':False}), 'utf-8'))


proxy = Proxy()
proxy()
pc = ProxyChecker()
pc()