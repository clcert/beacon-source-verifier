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
    pass


class Infura():
    NAME = "infura"

    def __init__(self, token):
        self.url = "https://mainnet.infura.io/v3/"
        self.token = token

    def get_latest_block(self, timeout=0) -> (Block, Block):
        r = requests.post(self.url + self.token, json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", False],
            "id": "1",
        }, timeout=timeout)
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

    def get_latest_block(self, timeout=0) -> (Block, Block):
        r = requests.get(self.url.format(self.token), timeout=timeout)
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

    def get_latest_block(self, timeout=0) -> (Block, Block):
        r = requests.post(self.url.format(self.token), json={
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": ["latest", False],
            "id": "1",
        }, timeout=timeout)
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
        self.fetch_interval = 6
        self.threshold = max(config.get("threshold", 1), 1)
        self.block_id_module = config.get("block_id_module", 1)
        for api in Source.REGISTERED_APIS:
            token = config.get("tokens", {}).get(f"{api.NAME}", None)
            if token is not None:
                self.sources[api.NAME] = api(token)
                self.buffers[api.NAME] = Buffer(Source.BUFFER_SIZE)
        if len(self.sources) < self.threshold:
            raise NotEnoughAPIsException()
        super().__init__()

    async def verify(self, params: map) -> map:
        valid = False
        reason = ""
        status = params.get("status", 1)
        if status != 0:
            reason = f"wrong status code: {status}"
        else:
            block_num = int(params["metadata"], 16)
            if block_num % self.block_id_module == 0:
                correct = 0
                errors = []
                possible = {}
                for k, buffer in self.buffers.items():
                    possible[k] = buffer.total_hashes()
                    if buffer.check_marker(block_num):
                        block = buffer.get_first()
                        if params["raw"] in block.hashes:
                            correct += 1
                        else:
                            error = f"block hash not found in block generation. block_number={block_num} block_hash={params['raw']} source_name={k} source_buffer_length={len(buffer)} source_buffer={buffer}"
                            errors.append(error)
                            log.debug(error)
                    else:
                        error = f"block number not found on buffer. block_number={block_num} source_name={k} source_buffer_length={len(buffer)} source_buffer={buffer}"
                        errors.append(error)
                        log.debug(error)
                if correct >= self.threshold:
                    valid = True
                    reason = f"possible={json.dumps(possible)}"
                else:
                    reason = f"not enough valid nodes to verify. total_nodes={len(self.buffers)} threshold={self.threshold} correct={correct} errors=[{','.join(errors)}]"
            else:
                reason = f"beacon reported an invalid block number as randomness source. module={self.block_id_module} block_id={block_num}"
        return {self.name(): {
            "valid": valid,
            "reason": reason
        }}

    async def init_collector(self) -> None:
        self.running = True

    async def collect(self) -> None:
        timeout = self.fetch_interval//len(self.sources)
        while self.running:
            start_time = datetime.now()
            for api in self.sources.values():
                log.debug(
                    f"Fetching latest ethereum block from {api.NAME} (timeout: {timeout})")
                try:
                    block, ancestor = api.get_latest_block(timeout)
                    if block.number % self.block_id_module == 0: 
                        self.buffers[api.NAME].add(block)
                    elif block.number % self.block_id_module == 1:
                        self.buffers[api.NAME].add(ancestor)
                except Exception as e:
                    log.error(f"error getting block from {api.NAME}: {e}")
            wait_time = max(0, self.fetch_interval -
                            (datetime.now() - start_time).seconds)
            log.debug(f"waiting {wait_time} seconds to fetch again")
            await asyncio.sleep(wait_time)
        log.debug("collector ended")

    async def finish_collector(self) -> None:
        self.running = False
