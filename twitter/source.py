from core.abstract_source import AbstractSource
from core.heap_buffer import HeapBuffer

import requests
import json
from requests.auth import AuthBase

from twitter.tweet import Tweet

STREAM_URL = "https://api.twitter.com/labs/1/tweets/stream/sample"


class BearerTokenAuth(AuthBase):
    def __init__(self, consumer_key, consumer_secret):
        self.bearer_token_url = "https://api.twitter.com/oauth2/token"
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.bearer_token = self.get_bearer_token()

    def get_bearer_token(self):
        response = requests.post(
            self.bearer_token_url,
            auth=(self.consumer_key, self.consumer_secret),
            data={'grant_type': 'client_credentials'},
            headers={"User-Agent": "TwitterDevSampledStreamQuickStartPython"})

        if response.status_code is not 200:
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        return r


class TwitterSource(AbstractSource):
    BUFFER_SIZE = 26 * 1000 * 2 * 5
    NAME = "twitter"
    ID = 2

    def __init__(self, config: map):
        self.key = config["consumer_key"]
        self.secret = config["consumer_secret"]
        self.tweet_interval = config["tweet_interval"]
        super().__init__(HeapBuffer(self.BUFFER_SIZE, self.create_heap_item))

    async def verify(self, params: map) -> map:
        pass

    async def init_collector(self) -> None:
        bearer_token = BearerTokenAuth(self.key, self.secret)
        self.response = requests.get(STREAM_URL, auth=bearer_token, headers={"User-Agent": "RandomVerifier-Python"},
                                     stream=True)

    async def collect(self) -> None:
        for response_line in self.response.iter_lines():
            if response_line:
                t = json.loads(response_line)["data"]
                self.buffer.add(Tweet(t["id"], t["created_at"], t["author_id"], t["text"]))

    async def finish_collector(self) -> None:
        self.response.close()

    def create_heap_item(self, item: Tweet) -> tuple:
        return item.id, item
