import asyncio
import datetime
import logging
from typing import List

from core.abstract_source import AbstractSource

import requests
import json
from requests.auth import AuthBase

from twitter.buffer import TwitterBuffer
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
        self.buffer = TwitterBuffer(self.BUFFER_SIZE)
        super().__init__()

    async def verify(self, params: map) -> map:
        their_list = parse_tweet_list(params["event"])
        start_date = datetime.datetime.strptime(params["metadata"], "%a %b %d %H:%M:%S +0000 %Y")
        end_date = start_date + datetime.timedelta(seconds=10)
        if self.buffer.check_marker(start_date):
            our_list = self.buffer.get_list()


        else:
            if self.buffer.check_marker(params["metadata"]):
                while len(self.buffer) < self.FRAMES_NUM:
                    logging.debug(
                        f"we need {self.FRAMES_NUM} frames to generate randomness but we have {len(self.buffer)}, waiting 5 seconds...")
                    await asyncio.sleep(5)
                frames = self.buffer.get_list(self.FRAMES_NUM)
                d = b''
                logging.debug(f"joining raw data from {len(frames)} frames...")
                for frame in frames:
                    d += frame.get_raw_data()
                logging.debug(f"Data joined, comparing with event data:")
                d_hex = d.hex()
                if d_hex == params["event"]:
                    return {self.name(): True}
        return {self.name(): False}

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


def parse_tweet_list(tweet_list: str) -> List[Tweet]:
    tweets = []
    try:
        tweet_json_list = json.loads(tweet_list)
        for tweet_json in tweet_json_list:
            t = tweet_json["data"]
            tweets.append(Tweet(t["id"], t["created_at"], t["author_id"], t["text"]))
    except Exception as e:
        logging.error(f"cannot parse tweet list: {e}")
    return tweets
