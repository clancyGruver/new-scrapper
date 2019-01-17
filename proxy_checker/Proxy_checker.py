import asyncio
import redis
import json
import aiohttp
from colorama import Back, Fore


class ProxyChecker:
    test_url = "http://ya.ru"
    timeout_sec = 10

    # read the list of proxy IPs in proxyList from the first Argument given
    r = redis.Redis()
    redis_key = 'proxy'
    proxy_list = []

    async def is_bad_proxy(self, ipport):
        async with aiohttp.ClientSession() as client:
            try:
                async with client.get(self.test_url, proxy=ipport, timeout=self.timeout_sec) as resp:
                    if resp.status == 200:
                        self.r.rpush('proxy', bytes(json.dumps({'proxy':ipport, 'checked': True}), 'utf-8'))
                        print(Fore.GREEN + "Working:", ipport + Fore.RESET)
                    else:
                        print(Fore.RED + "Not Working:", ipport + Fore.RESET)
            except:
                print(Back.LIGHTRED_EX + ipport + Back.RESET)

    def __init__(self):
        l = self.r.llen(self.redis_key)
        while l > 0:
            l -= 1
            item = json.loads(self.r.lpop(self.redis_key).decode('utf-8'))
            if item['checked'] == False:
                self.proxy_list.append(item)
            else:
                self.r.rpush('proxy', bytes(json.dumps(item), 'utf-8'))

    def __call__(self):
        print(Back.GREEN + "Starting... \n" + Back.RESET)
        loop = asyncio.get_event_loop()
        tasks = []
        for item in self.proxy_list:
            proxy = "http://" + item['proxy']
            tasks.append(asyncio.ensure_future(self.is_bad_proxy(proxy)))
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
        print(Back.GREEN + "\n...Finished" + Back.RESET)