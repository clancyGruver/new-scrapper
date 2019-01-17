import multiprocessing
import re
import socket
import threading
import time
from urllib import robotparser
from urllib.parse import urljoin, urlparse
from downloader import Downloader
from redis_queue import RedisQueue


SLEEP_TIME = 1
socket.setdefaulttimeout(60)


def get_robots_parser(robots_url):
    """
        Return the robots parser object using the robots_url
    """
    try:
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        rp.read()
        return rp
    except Exception as e:
        print('Error finding robots_url:', robots_url, e)


def clean_link(url, domain, link):
    if link.startswith('//'):
        link = '{}:{}'.format(urlparse(url).scheme, link)
    elif link.startswith('://'):
        link = '{}{}'.format(urlparse(url).scheme, link)
    else:
        link = urljoin(domain, link)
    return link

def threaded_crawler_rq(crawl_queue=None, delay=3, max_depth=4, num_retries=2, cache={}, max_threads=10,
                        scraper_callback=None):
    """ Crawl from the given start URLs following links matched by link_regex. In this
        implementation, we do not actually scrape any information.

        args:
            start_url (str or list of strs): web site(s) to start crawl
            link_regex (str): regex to match for links
        kwargs:
            user_agent (str): user agent (default: wswp)
            proxies (list of dicts): a list of possible dicts
                for http / https proxies
                For formatting, see the requests library
            delay (int): seconds to throttle between requests to one domain
                        (default: 3)
            max_depth (int): maximum crawl depth (to avoid traps) (default: 4)
            num_retries (int): # of retries when 5xx error (default: 2)
            cache (dict): cache dict with urls as keys
                          and dicts for responses (default: {})
            scraper_callback: function to be called on url and html content
    """
    if crawl_queue is None:
        crawl_queue = RedisQueue()
    # keep track which URL's have seen before
    robots = {}
    downloader = Downloader(delay=delay, cache=cache)

    def process_queue():
        while len(crawl_queue):
            url = crawl_queue.pop()
            no_robots = False
            if not url or 'http' not in url:
                continue
            domain = '{}://{}'.format(urlparse(url).scheme,
                                      urlparse(url).netloc)
            # check url passes robots.txt restrictions
            depth = crawl_queue.get_depth(url)
            if depth == max_depth:
                print('Skipping %s due to depth' % url)
                continue
            html = downloader(url, num_retries=num_retries)
            if not html:
                continue
            if scraper_callback:
                links = scraper_callback(url, html) or []
            else:
                links = []
            # filter for links matching our regular expression
            for link in links:
                if 'http' not in link:
                    link = clean_link(url, domain, link)
                crawl_queue.push(link)
                crawl_queue.set_depth(link, depth + 1)

    # wait for all download threads to finish
    threads = []
    while threads or len(crawl_queue):
        for thread in threads:
            if not thread.is_alive():
                threads.remove(thread)
        while len(threads) < max_threads and crawl_queue:
            # can start some more threads
            thread = threading.Thread(target=process_queue)
            thread.setDaemon(True)  # set daemon so main thread can exit w/ ctrl-c
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        time.sleep(SLEEP_TIME)


def mp_threaded_crawler(*args, **kwargs):
    """ create a multiprocessing threaded crawler """
    processes = []
    num_procs = kwargs.pop('num_procs')
    if not num_procs:
        num_procs = multiprocessing.cpu_count()
    for _ in range(num_procs):
        proc = multiprocessing.Process(target=threaded_crawler_rq,
                                       args=args, kwargs=kwargs)
        proc.start()
        processes.append(proc)
    # wait for processes to complete
    for proc in processes:
        proc.join()


if __name__ == '__main__':
    from beletag_callback import BeletagCallback
    from redis_cache import RedisCache
    import argparse

    parser = argparse.ArgumentParser(description='Multiprocessing threaded link crawler')
    parser.add_argument('max_threads', type=int, help='maximum number of threads',
                        nargs='?', default=20)
    parser.add_argument('num_procs', type=int, help='number of processes',
                        nargs='?', default=None)
    parser.add_argument('url_pattern', type=str, help='regex pattern for url matching',
                        nargs='?', default='$^')
    par_args = parser.parse_args()

    """
    AC = AlexaCallback()
    AC()
    """
    print("Start parsing First_level urls")
    Beletag = BeletagCallback()
    Beletag()
    start_time = time.time()

    mp_threaded_crawler(cache=RedisCache(), num_procs=par_args.num_procs, max_threads=par_args.max_threads,
                        crawl_queue=Beletag.pages_queue, scraper_callback=Beletag.page_parse)
    first_level_time = time.time()
    print('First_level urls finished: %ss' % (first_level_time - start_time))

    print('Elements scraping start')

    mp_threaded_crawler(cache=RedisCache(), num_procs=par_args.num_procs, max_threads=par_args.max_threads,
                        crawl_queue=Beletag.elements_queue, scraper_callback=Beletag.element_parse)

    elements_time = time.time()
    print('Elements scraping finished: %ss' % (elements_time - first_level_time))
    print('Totla time: %ss' % (elements_time - start_time))

