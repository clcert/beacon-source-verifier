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
from earthquake.buffer import Buffer
from earthquake.event import Event

log = logging.getLogger(__name__)


class Source(AbstractSource):
    BUFFER_SIZE = 500 * 10 
    NAME = "earthquake"

    def __init__(self, config: map):
        self.source_url = config["source_url"]
        self.fetch_interval = config["fetch_interval"]
        self.buffer = Buffer(100)
        self.running = False
        super().__init__()

    async def verify(self, params: map) -> map:
        reason = ""
        valid = False
        if self.buffer.check_marker(params["metadata"]):
            our_event = self.buffer.get_first()
            their_event = parse_json_event(params["event"])
            log.debug(f"Comparing our event data with their event data:")
            if our_event == their_event:
                valid = True
            else:
                reason = f"event value does not match. ours={our_event} theirs={their_event}"
        else:
            reason = f"metadata \"{params['metadata']}\" not found. buffer_size={len(self.buffer)}"
        return {
            self.name(): {
                "valid": valid,
                "reason": reason
                }
            }

    async def init_collector(self) -> None:
        self.running = True

    async def collect(self) -> None:
        while self.running:
            start_time = datetime.now()
            res = requests.get(self.source_url)
            soup = BeautifulSoup(res.content, 'html.parser')
            trs = soup.find_all("tr")[1:]
            if len(trs) != 0:
                seism = self.parse_seism(trs[0])
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


    def parse_seism(self, tr):
        tds = tr.find_all("td")
        if len(tds) != 8:
            return None
        url = urljoin(self.source_url, tds[0].find("a", href=True).attrs["href"])
        # Getting data from that URL:
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'html.parser')
        child_tds = soup.find_all("td")
        if len(child_tds) != 14:
            return None
        id = url.split("/")[-1].split(".html")[0]
        string_data = [id]
        for i in range(3, len(child_tds) - 2, 2): # skipping local time and last td
            string_data.append(child_tds[i].text)
        # we need to strip magnitude unit and source from magnitude field
        string_data[-1] = string_data[-1].split(" ")[0]
        # we need to strip depth unit from depth field
        string_data[-2] = string_data[-2].split(" ")[0]
        return Event(*string_data)  

def parse_json_event(str_event: str) -> Event:
    ev = json.loads(str_event)
    return Event(ev["id"], ev["utc"], ev["latitude"], ev["longitude"], ev["depth"], ev["magnitude"])
