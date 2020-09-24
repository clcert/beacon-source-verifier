import datetime
import json
import logging
from typing import List

import requests
from requests.auth import AuthBase

from core.source_manager import SourceManager
from core.results import VerifierException, VerifierResult

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
            raise TwitterCollectorException(
                f"Cannot get a Bearer token (HTTP {response.status_code}): {response.text}")

        body = response.json()
        return body['access_token']

    def __call__(self, r):
        r.headers['Authorization'] = f"Bearer %s" % self.bearer_token
        return r


class Source(AbstractSource):
    STREAM_URL = "https://api.twitter.com/2/tweets/sample/stream?tweet.fields=created_at&expansions=author_id"
    BUFFER_SIZE = 20000
    NAME = "twitter"

    def __init__(self, config: map, mgr: SourceManager):
        self.key = config["consumer_key"]
        self.secret = config["consumer_secret"]
        self.tweet_interval = config["tweet_interval"]
        self.second_start = config["second_start"]
        self.buffer = Buffer(mgr.metrics.collector_buffer_size.labels(self.name()), self.second_start, self.BUFFER_SIZE)
        self.response = None
        super().__init__(mgr)

    async def verify(self, params: map) -> map:
        result = VerifierResult(self.name())
        result.possible = len(self.get_possible())
        status = params.get("status", 2)
        result.ext_value_status = status
        if (status & 2) == 2:
            result.status_code = 240
            result.add_detail(
                f"ExtValue is not valid."
                f"status={status}")
        else:  
            their_list = parse_tweet_list(params["raw"])
            start_date = datetime.datetime.fromisoformat(params["metadata"][:-1])
            end_date = start_date + datetime.timedelta(seconds=self.tweet_interval)
            if start_date.second != self.second_start:
                result.status_code = 220
                result.add_detail(
                    f"Marker did not start in expected second.",
                    f"second={self.second_start}")
            elif len(their_list) == 0:
                result.status_code = 222
                result.add_detail("Beacon reported an empty tweet list")
            elif self.buffer.check_marker(start_date):
                our_list = self.buffer.get_list(end_date)
                if len(our_list) == 0:
                    result.status_code = 222
                    result.add_detail("Verifier reported an empty tweet list")
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
                    self.manager.metrics.twitter_extra_tweets.labels('verifier').observe(len(our_uniq))
                    self.manager.metrics.twitter_extra_tweets.labels('beacon').observe(len(their_uniq))
                    if len(our_uniq) > 0 or len(their_uniq) > 0:
                        result.code = 221
                        result.add_detail(
                            f"Some items are not on both lists",
                            f"our_buf_len={len(our_list)} ",
                            f"their_buf_len={len(their_list)} ",
                            f"our_interval={our_list[0].datestr}_{our_list[-1].datestr} ",
                            f"their_interval={their_list[0].datestr}_{their_list[-1].datestr} ",
                            f"our_uniq=[{','.join([str(x) for x in our_uniq])}] ",
                            f"their_uniq=[{','.join([str(x) for x in their_uniq])}]")
            else:
                result.status_code = 222
                result.add_detail(
                    f"metadata not found",
                    f"metadata={params['metadata']}",
                    f"buffer_size={len(self.buffer)}")
        result.finish()
        return result

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
                    raise TwitterCollectorException(f"{resp}")
                t = resp["data"]
                tweet = Tweet(t["id"], t["created_at"],
                              t["author_id"], t["text"])
                start_date = tweet.date.replace(second=self.second_start)
                end_date = start_date + \
                    datetime.timedelta(seconds=self.tweet_interval)
                if tweet.date >= start_date and tweet.date <= end_date:
                    self.buffer.add(tweet)
        print("collector ended :(")

    async def finish_collector(self) -> None:
        self.response.close()

    def get_possible(self) -> List[str]:
        return [s for s in self.buffer.possible]


def parse_tweet_list(tweet_list: str) -> List[Tweet]:
    tweets = []
    try:
        if len(tweet_list) == 0:
            log.error("empty tweet list")
        else:
            tweet_json_list = json.loads(tweet_list)
            if tweet_json_list is not None:
                for t in tweet_json_list:
                    tweets.append(Tweet(t["id"], t["created_at"],
                                        t["author_id"], t["text"]))
    except Exception as e:
        log.error(f"cannot parse tweet list: {e}")
    return tweets
