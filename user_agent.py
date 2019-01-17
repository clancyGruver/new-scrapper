import redis
import json
from random import randint

class UserAgent:
    def __init__(self):
        self.r = redis.Redis()

    def __call__(self, *args, **kwargs):
        rint = randint(0, self.r.llen('user_agent')-1)
        return json.loads(self.r.lindex('user_agent', rint).decode('utf-8'))