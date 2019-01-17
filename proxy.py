import redis
import json

class Proxy:
    redis_key = "proxy"

    def __init__(self):
        self.r = redis.Redis()

    def __call__(self, *args, **kwargs):
        flag = True
        while flag:
            item = json.loads(self.r.lpop(self.redis_key).decode('utf-8'))
            self.r.rpush(self.redis_key, bytes(json.dumps(item), 'utf-8'))
            if item['checked'] == False:
                continue
            else:
                return {'http':item['proxy']}