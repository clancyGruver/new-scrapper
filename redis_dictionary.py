import json
from datetime import datetime, timedelta
from redis import StrictRedis

class RedisDictionary:
    """ RedisCache helps store urls and their responses to Redis
        Initialization components:
            client: a Redis client connected to the key-value database for
                the webcrawling cache (if not set, a localhost:6379
                default connection is used).
            expires (datetime.timedelta): timedelta when content will expire
                (default: 1 day ago)
            encoding (str): character encoding for serialization
            compress (bool): boolean indicating whether compression with zlib should be used
    """
    def __init__(self, client=None, expires=timedelta(days=1), encoding='utf-8'):
        self.client = (StrictRedis(host='localhost', port=6379, db=2)
                       if client is None else client)
        self.expires = expires
        self.encoding = encoding

    def __getitem__(self, dict):
        """Load data from Redis for given dictionary"""
        record = self.client.hgetall(dict)
        if record:
            return record
        else:
            # URL has not yet been cached
            raise KeyError(dict + ' does not exist')

    def __setitem__(self, dict, data):
        """Save data to Redis for given dictionary"""
        self.client.hmset(dict, data)
