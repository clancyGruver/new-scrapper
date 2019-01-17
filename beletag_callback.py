from redis_queue import RedisQueue
from redis_dictionary import RedisDictionary
import redis
from lxml.html import fromstring
from urllib.parse import urlparse
import json
import re
import os
import requests

class BeletagCallback:
    def __init__(self):
        self.q_pages_name = 'beletag_pages'
        self.q_elements_name = 'beletag_elements'
        self.redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)
        self.redisClientElements = redis.StrictRedis(host='localhost', port=6379, db=2)
        self.pages_queue = RedisQueue(client=self.redisClient, queue_name=self.q_pages_name)
        self.elements_queue = RedisQueue(client=self.redisClient, queue_name=self.q_elements_name)
        self.main_url = "https://shop.beletag.com/catalog/"
        self.categories = {
            "Мужское": {
                "485": "Брюки,бриджи",
                "771": "Спорт",
                "486": "Спорт",
                "559": "Бельевая группа",
                "487": "Шорты",
                "489": "Джемпера",
                "490": "Футболки",
                "634": "Поло",
                "715": "Комплекты"
            },
            "Женское": {
                "734": "Базовый ассортимент",
                "602": "Бельевая группа",
                "498": "Блузки, рубашки",
                "599": "Брюки, бриджи",
                "601": "Водолазки",
                "500": "Джемпера",
                "474": "Майки",
                "496": "Платья, сарафаны",
                "770": "Спорт",
                "598": "Толстовки, куртки",
                "497": "Футболки",
                "501": "Шорты",
                "502": "Юбки"
            },    
            "Общее": {
                "510": "Одежда для дома",
                "661": "Термобельё"
            }         
        }

    def __call__(self):
        self.set_categories_pages()

    def set_categories_pages(self):
        for cat in self.categories:
            for catId in self.categories[cat]:
                url = 'https://shop.beletag.com/catalog/{}/?pagecount=90&mode=ajax&PAGEN_1=1'.format(catId)
                self.pages_queue.push(url)

    def page_parse(self, url, html):
        tree = fromstring(html)
        parsed_url = urlparse(url).path.split('/')
        cat_id = parsed_url[-2]
        if not cat_id:
            return

        #parse first_level_urls
        a_count = tree.xpath('//div[@class="pages"][1]/a')
        if len(a_count) > 0:
            pages_count = int(a_count[-1].text.strip())
        else:
            pages_count = 1
        print(url, 'pages_count: {}'.format(pages_count))
        for x in range(1, pages_count + 1):
            ready_url = self.main_url + "{}/?pagecount=90&mode=ajax&PAGEN_1={}".format(cat_id, str(x))
            self.pages_queue.push(ready_url)

        # parse elements_urls
        links = tree.xpath('//a[@class="base-photo"]/@href')
        if len(links) > 0:
            for link in links:
                url_params = urlparse(link).path.split('/')
                category_id = url_params[2]
                element_id = url_params[3]
                ready_url = self.main_url + "{}/{}/".format(category_id, element_id)
                self.elements_queue.push(ready_url)

    # Сбор всей информации о товаре и занесение в массив с результатом
    def element_parse(self, url, html):
        rd = RedisDictionary(self.redisClientElements)
        parsed_url = urlparse(url).path.split('/')
        element_id = parsed_url[-2]
        result = {'url': url}
        tree = fromstring(html)
        catalog_element = tree.xpath('//div[@class="catalog-element"]')

        if len(catalog_element) == 0:
            return 0

        catalog_element = catalog_element[0]
        cn = catalog_element.xpath('//div[@class="catalog-element"]/span[@itemprop="category"]/@content')[0]
        cn = cn.split(" > ")[-1:]
        category_name = "".join(cn)
        good_id = catalog_element.attrib['data-id']
        category_id = catalog_element.attrib["data-categoryid"]

        try:
            img = catalog_element.xpath("//div[@class='photo-zoom']/img/@src")[0]
        except Exception:
            img = None

        self.save_image(img, good_id + '.' + "".join(img.split('.')[-1:]))

        scripts = tree.xpath("//script/text()")
        stocks = None
        for script in scripts:
            if script is None:
                continue
            stock = re.search('offersQuantity\[[0-9]+\]=(.*?);', script)
            if stock is None:
                continue
            else:
                stocks = json.loads(stock.groups()[0])
                break

        try:
            info = tree.xpath('//div[@id="item-full-zoom"]')[0]
        except Exception:
            info = ''

        try:
            name = info.xpath('//div[@id="item-full-zoom"]/div[@class="title"]/span[@itemprop="name"]/text()')[0]
        except Exception:
            name = ''

        try:
            articul = info.xpath('//div[@class="article"]/span/@content')[0]
        except Exception:
            articul = ''

        try:
            compositions = info.xpath('//div[@class="composition"]')
            complecte = ''
            season = ''
            for composition in compositions:
                for comp in composition.getchildren():
                    if "Состав" in comp.text:
                        complecte = comp.tail
                    elif "Сезон" in comp.text:
                        season = comp.tail
        except Exception:
            complecte = ''
            season = ''

        try:
            description = info.xpath('//div[@class="description"]/text()')[0]
            #description = description.replace(u'\xa0', u' ')
            #description = description.strip()
        except Exception:
            description = ''

        try:
            price = info.xpath('//div[contains(@class, "price")]/div[contains(@class, "price")]/text()')
            if not price[-1]:
                price = price[0]
            else:
                price = price[-1]
        except Exception:
            price = ''

        colors = []

        try:
            colors_container = info.xpath('//div[@class="colors"]/div')
            for color in colors_container:
                colors.append({"name": color.text, "id": color.attrib["data-id"]})

            sizes = []
            sizes_container = info.xpath('//div[@class="sizes"]/div')
            for size in sizes_container:
                sizes.append({"name": size.text[1:], "id": size.attrib["data-id"]})
        except Exception:
            pass

        result = {
            "category":
            {
                "name": category_name,
                "id": category_id
            },
            "item":
            {
                "name": name,
                "id": good_id
            },
            "articul": articul,
            "season": season,
            "complecte": complecte,
            "description": description,
            "price": price,
            "colors": []
        }

        for color in colors:
            tmp_color = {
                "id": color["id"],
                "name": color["name"],
                "size": []
            }
            for size in sizes:
                # size count
                c = 0
                if (color["id"] in stocks) and (size["id"] in stocks[color["id"]]):
                    c = stocks[color["id"]][size["id"]]
                tmp_color["size"].append({
                    "id": size["id"],
                    "name": size["name"],
                    "count": c
                })
            result["colors"].append(tmp_color)
        rd[element_id] = result

    def save_image(self, img_url, name):
        directory = os.getcwd() + "/images/beletag/"
        file = directory + name
        if os.path.exists(file):
            return
        if not os.path.exists(directory):
            os.makedirs(directory)

        url = 'http://shop.beletag.com/' + img_url
        img = requests.get(url, stream=True)
        with open(file, "bw") as f:
            for chunk in img.iter_content(8192):
                f.write(chunk)