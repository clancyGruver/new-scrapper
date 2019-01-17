#import multiprocessing
import socket
import time
from downloader import Downloader
import asyncio
import redis
from beletag_callback import BeletagCallback
from redis_cache import RedisCache

SLEEP_TIME = 1
socket.setdefaulttimeout(60)


class Beletag:
    def __init__(self):
        self.cb = BeletagCallback()
        self.cb.setCategoriesPages()
        self.redisClient = redis.StrictRedis(host='localhost', port=6379, db=1)
        self.cache = RedisCache(client=self.redisClient)
        self.max_tasks = 20

    def getAllPages(self):
        loop = asyncio.get_event_loop()
        tasks = []
        D = Downloader(cache=self.cb.redisClientPages)

        async def process_queue(url):
            while len(self.cb.pages_queue):
                if not url or 'http' not in url:
                    continue
                html = D(url, num_retries=5)
                if not html:
                    continue
                self.cb.pageParse(url, html)

        while tasks or len(self.cb.pages_queue):
            while len(tasks) < self.max_tasks and len(self.cb.pages_queue):
                tasks.append(asyncio.ensure_future(process_queue(self.cb.pages_queue.pop())))
            time.sleep(SLEEP_TIME)
            loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
""""
    def mp_threaded_crawler(*args, **kwargs):
        # create a multiprocessing threaded crawler
        processes = []
        num_procs = multiprocessing.cpu_count()
        for _ in range(num_procs):
            proc = multiprocessing.Process(target=threaded_crawler_rq,
                                           args=args, kwargs=kwargs)
            proc.start()
            processes.append(proc)
        # wait for processes to complete
        for proc in processes:
            proc.join()
"""


if __name__ == '__main__':
    start_time = time.time()
    b = Beletag()
    b.getAllPages()
    print('Total time: %ss' % (time.time() - start_time))
