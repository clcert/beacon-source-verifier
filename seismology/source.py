from datetime import datetime
import json
import logging
from typing import List
from bs4 import BeautifulSoup
import asyncio

import requests
from requests.auth import AuthBase

from core.abstract_source import AbstractSource
from seismology.buffer import SeismBuffer
from seismology.seism import Seism

log = logging.getLogger(__name__)


class SeismSource(AbstractSource):
    BUFFER_SIZE = 500 * 10  # (~120 seconds)
    NAME = "seismology"

    def __init__(self, config: map):
        self.source_url = config["source_url"]
        self.fetch_interval = config["fetch_interval"]
        self.buffer = SeismBuffer(100)
        self.running = False
        super().__init__()

    async def verify(self, params: map) -> map:
        seism = self.buffer.get_last()
        d = seism.get_raw_data()
        log.debug(f"Comparing our seism data with event data:")
        d_hex = d.hex()
        if d_hex == params["event"]:
            return {self.name(): True}
        return {self.name(): False}

    async def init_collector(self) -> None:
        self.running = True

    async def collect(self) -> None:
        while self.running:
            start_time = datetime.now()
            res = requests.get(self.source_url)
            soup = BeautifulSoup(res.content, 'html.parser')
            trs = soup.find_all("tr")[1:]
            if len(trs) > 0:
                seism = parse_seism(trs[0])
                if seism is not None:
                    self.buffer.add(seism)
                else:
                    log.debug(f"Cannot parse last seism")
            else:
                log.error(f"cannot get seism list")
            wait_time = max(0, self.fetch_interval - (datetime.now() - start_time).seconds)
            log.debug(f"waiting {wait_time} seconds to fetch again")
            await asyncio.sleep(wait_time)
        log.debug("collector ended")

    async def finish_collector(self) -> None:
        self.running = False


def parse_seism(tr):
    tds = tr.find_all("td")
    if len(tds) < 8:
        return None
    url = tds[0].find("a", href=True)
    id = url.attrs["href"].split("/")[-1].split(".html")[0]
    string_data = [id]
    for td in tds[1:-1]: # ignoring id and reference
        string_data.append(td.text)
    return Seism(*string_data)
    