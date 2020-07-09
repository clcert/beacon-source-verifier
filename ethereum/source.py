from datetime import datetime
import json
import logging
from typing import List
from bs4 import BeautifulSoup
import asyncio
from urllib.parse import urljoin

import requests
from requests.auth import AuthBase

from core.abstract_source import AbstractSource
from ethereum.buffer import Buffer

from ethereum.block import Block

log = logging.getLogger(__name__)


class APIException(Exception):
    def __init__(self, error):
        self.err = error

class NotEnoughAPIsException(Exception):
    def __init__(self, error):
        self.err = error


class Infura():
    NAME = "infura"

    def __init__(self, token):
        self.url = "https://mainnet.infura.io/v3/"
        self.token = token

    def get_latest_block(self) -> Block:
        r = requests.post(self.url + self.token, json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", False],
            "id": "1",
        })
        if r.status_code != 200:
            raise APIException(r.json())
        r_json = r.json()["result"]
        id = int(r_json["number"], 16)
        ancestor = Block(id-1, [uncle[2:] for uncle in r_json["uncles"]])
        ancestor.hashes.add(r_json["parentHash"][2:])
        return Block(id, [r_json["hash"][2:]]), ancestor

class EtherScan():
    NAME = "etherscan"

    def __init__(self, token):
        self.url = "https://api.etherscan.io/api?module=proxy&action=eth_getBlockByNumber&tag=latest&boolean=false&apikey={}"
        self.token = token

    def get_latest_block(self) -> Block:
        r = requests.get(self.url.format(self.token))
        if r.status_code != 200:
            raise APIException(r.json())
        r_json = r.json()["result"]
        id = int(r_json["number"], 16)
        ancestor = Block(id-1, [uncle[2:] for uncle in r_json["uncles"]])
        ancestor.hashes.add(r_json["parentHash"][2:])
        return Block(id, [r_json["hash"][2:]]), ancestor


class Rivet():
    NAME = "rivet"

    def __init__(self, token):
        self.url = "https://{}.eth.rpc.rivet.cloud/"
        self.token = token
    
    def get_latest_block(self) -> Block:
        r = requests.post(self.url.format(self.token), json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", False],
            "id": "1",
        })
        if r.status_code != 200:
            raise APIException(r.json())
        r_json = r.json()["result"]
        id = int(r_json["number"], 16)
        ancestor = Block(id-1, [uncle[2:] for uncle in r_json["uncles"]])
        ancestor.hashes.add(r_json["parentHash"][2:])
        return Block(id, [r_json["hash"][2:]]), ancestor


class Source(AbstractSource):
    BUFFER_SIZE = 120 
    NAME = "ethereum"
    REGISTERED_APIS = [
        Infura,
        EtherScan,
        Rivet
    ]
    def __init__(self, config: map):
        self.sources = {}
        self.buffers = {} 
        self.running = False
        self.fetch_interval = 5
        self.threshold = max(config.get("threshold", 1),1)
        for api in Source.REGISTERED_APIS:
            token = config.get(f"{api.NAME}_token", None)
            if token is not None:
                self.sources[api.NAME] = api(token)
                self.buffers[api.NAME] = Buffer(Source.BUFFER_SIZE)
        if len(self.sources) < self.threshold:
            raise NotEnoughAPIsException()
        super().__init__()

    async def verify(self, params: map) -> map:
        correct = 0
        for buffer in self.buffers:
            if buffer.check_marker(params["metadata"]):
                block = buffer.get_first()
                if params["event"] in block.hashes:
                    correct += 1
        if correct >= self.threshold:
            return {self.name(): True}
        return {self.name(): False}

    async def init_collector(self) -> None:
        self.running = True

    async def collect(self) -> None:
        while self.running:
            start_time = datetime.now()
            for api in self.sources.values():
                log.debug(f"Fetching latest ethereum block from {api.NAME}")
                try:
                    block, ancestor = api.get_latest_block()
                    self.buffers[api.NAME].add(ancestor)
                    self.buffers[api.NAME].add(block)
                except Exception as e:
                    log.debug(f"error getting block from {api.NAME}: {e}")
            wait_time = max(0, self.fetch_interval - (datetime.now() - start_time).seconds)
            log.debug(f"waiting {wait_time} seconds to fetch again")
            await asyncio.sleep(wait_time)
        log.debug("collector ended")

    async def finish_collector(self) -> None:
        self.running = False

