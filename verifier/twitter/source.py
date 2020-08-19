import datetime
import json
import logging
from typing import List

import requests
from requests.auth import AuthBase

from core.abstract_source import AbstractSource
from twitter.buffer import Buffer
from twitter.tweet import Tweet

log = logging.getLogger(__name__)


class TwitterCollectorException(Exception):

    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return f"TwitterCollectionException: {self.reason}"

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
            raise TwitterCollectorException(f"Cannot get a Bearer token (HTTP {response.status_code}): {response.text}")

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        return r


class Source(AbstractSource):
    STREAM_URL = "https://api.twitter.com/labs/1/tweets/stream/sample"
    BUFFER_SIZE = 20000
    NAME = "twitter"

    def __init__(self, config: map):
        self.key = config["consumer_key"]
        self.secret = config["consumer_secret"]
        self.tweet_interval = config["tweet_interval"]
        self.second_start = config["second_start"]
        self.buffer = Buffer(self.BUFFER_SIZE)
        self.response = None
        super().__init__()

    async def verify(self, params: map) -> map:
        valid = False
        reason = ""
        log.debug(f"checking twitter buffer... (length: {len(self.buffer)})")
        status = params.get("status", 1)
        if (status & 2) == 2 :
            reason = f"wrong status code: {status}"
        else:
            their_list = parse_tweet_list(params["raw"])
            start_date = datetime.datetime.fromisoformat(params["metadata"][:-1])
            end_date = start_date + datetime.timedelta(seconds=self.tweet_interval)
            if start_date.second != self.second_start:
                reason = f"marker did not start in second {self.second_start}"
            elif len(their_list) == 0:
                reason = "beacon reported an empty tweet list"
            elif self.buffer.check_marker(start_date):
                our_list = self.buffer.get_list(end_date)
                if len(our_list) == 0:
                    reason = "our verifier reported an empty tweet list"
                else:
                    i, j = 0, 0
                    our_uniq, their_uniq = [], []
                    while i < len(our_list) and j < len(their_list):
                        ours = our_list[i]
                        theirs = their_list[j]
                        if ours < theirs:
                            our_uniq.append(ours)
                            i += 1
                        elif theirs < ours:
                            their_uniq.append(theirs)
                            j += 1
                        else:
                            i += 1
                            j += 1
                    our_uniq += our_list[i:]
                    their_uniq += their_list[j:]
                    if len(our_uniq) > 0 or len(their_uniq) > 0:
                        reason = f"Some items are not on both lists. our_interval={our_list[0].datestr}_{our_list[-1].datestr} their_interval={their_list[0].datestr}_{their_list[-1].datestr} our_uniq=[{','.join([str(x) for x in our_uniq])}] their_uniq=[{','.join([str(x) for x in their_uniq])}]"
                    else: 
                        valid = True
                        reason = f"possible=1"
            else:
                reason = f"metadata \"{params['metadata']}\" not found. buffer_size={len(self.buffer)}"
        return {self.name(): {
            "valid": valid,
            "reason": reason
        }}

    async def init_collector(self) -> None:
        bearer_token = BearerTokenAuth(self.key, self.secret)
        self.response = requests.get(self.STREAM_URL, auth=bearer_token,
                                     headers={
                                         "User-Agent": "RandomVerifier-Python"},
                                     stream=True)

    async def collect(self) -> None:
        for response_line in self.response.iter_lines():
            if response_line:
                resp = json.loads(response_line)
                if "data" not in resp:
                    raise TwitterCollectorException(f"{resp['title']}: {resp['detail']}")
                t = resp["data"]
                tweet = Tweet(t["id"], t["created_at"], t["author_id"], t["text"])
                start_date = tweet.date.replace(second=self.second_start)
                end_date = start_date + datetime.timedelta(seconds=self.tweet_interval)
                if tweet.date >= start_date and tweet.date <= end_date:
                    self.buffer.add(tweet)
        print("collector ended :(")

    async def finish_collector(self) -> None:
        self.response.close()


def parse_tweet_list(tweet_list: str) -> List[Tweet]:
    tweets = []
    try:
        tweet_json_list = json.loads(tweet_list)
        for t in tweet_json_list:
            tweets.append(Tweet(t["id"], t["created_at"],
                                t["author_id"], t["text"]))
    except Exception as e:
        log.error(f"cannot parse tweet list: {e}")
    return tweets
