import datetime
import json
import logging
from typing import List

import requests
from requests.auth import AuthBase

from core.abstract_source import AbstractSource
from twitter.buffer import TwitterBuffer
from twitter.tweet import Tweet

log = logging.getLogger(__name__)


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

        if response.status_code != 200:
            raise Exception(f"Cannot get a Bearer token (HTTP %d): %s" % (response.status_code, response.text))

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        return r


class TwitterSource(AbstractSource):
    STREAM_URL = "https://api.twitter.com/labs/1/tweets/stream/sample"
    BUFFER_SIZE = 500 * 10  # (~120 seconds)
    NAME = "twitter"
    ID = 2

    def __init__(self, config: map):
        self.key = config["consumer_key"]
        self.secret = config["consumer_secret"]
        self.tweet_interval = config["tweet_interval"]
        self.buffer = TwitterBuffer(self.BUFFER_SIZE)
        self.response = None
        super().__init__()

    async def verify(self, params: map) -> map:
        equal = False
        log.error(f"Checking twitter buffer... (length: {len(self.buffer)})")
        their_list = parse_tweet_list(params["event"])
        start_date = datetime.datetime.fromisoformat(params["metadata"][:-1])
        end_date = start_date + datetime.timedelta(seconds=10)
        if self.buffer.check_marker(start_date):
            our_list = self.buffer.get_list(end_date)
            if len(our_list) == len(their_list):
                equal = True
                for ours, theirs in zip(our_list, their_list):
                    if ours != theirs:
                        log.error(f"different lists!  ours: {ours} theirs: {theirs}")
                        equal = False
                        break
        return {self.name(): equal}

    async def init_collector(self) -> None:
        bearer_token = BearerTokenAuth(self.key, self.secret)
        self.response = requests.get(self.STREAM_URL, auth=bearer_token,
                                     headers={"User-Agent": "RandomVerifier-Python"},
                                     stream=True)

    async def collect(self) -> None:
        for response_line in self.response.iter_lines():
            if response_line:
                t = json.loads(response_line)["data"]
                self.buffer.add(Tweet(t["id"], t["created_at"], t["author_id"], t["text"]))

    async def finish_collector(self) -> None:
        self.response.close()


def parse_tweet_list(tweet_list: str) -> List[Tweet]:
    tweets = []
    try:
        tweet_json_list = json.loads(tweet_list)
        for t in tweet_json_list:
            tweets.append(Tweet(t["id"], t["created_at"], t["author_id"], t["text"]))
    except Exception as e:
        log.error(f"cannot parse tweet list: {e}")
    return tweets
