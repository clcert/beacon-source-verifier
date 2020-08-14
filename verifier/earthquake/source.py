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

class SeismParsingException(Exception):
    def __init__(self, reason):
        self.reason = reason

    def __str__(self):
        return f"SeismParsingException: {self.reason}"

class Source(AbstractSource):
    BUFFER_SIZE = 100
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
        status = params.get("status", 1)
        possible = len(self.buffer)
        if status != 0:
            reason = f"wrong status code: {status}"
        elif self.buffer.check_marker(params["metadata"]):
            our_event = self.buffer.get_first()
            their_event = parse_json_event(params["raw"])
            log.debug(f"Comparing our event data with their event data:")
            if our_event == their_event:
                valid = True
                reason = f"possible={possible}"
            else:
                reason = f"event value does not match. ours={our_event} theirs={their_event}"
        else:
            reason = f"metadata \"{params['metadata']}\" not found. buffer={self.buffer}"
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
            trs.reverse() # To add them in chronological order
            trs
            if len(trs) != 0:
                for tr in trs:
                    try:
                        seism = self.parse_seism(tr)
                        if not seism.is_erb():
                            self.buffer.add(seism)
                    except Exception as e:
                        log.error(f"Error parsing seism: {e}")
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
            raise SeismParsingException(f"not enough columns in seism summary page.")
        url = urljoin(self.source_url, tds[0].find("a", href=True).attrs["href"])
        # Getting data from that URL:
        res = requests.get(url)
        soup = BeautifulSoup(res.content, 'html.parser')
        child_tds = soup.find_all("td")
        if len(child_tds) != 14:
            raise SeismParsingException(f"not enough fields in seism page. seism={url}")
        id = url.split("/")[-1].split(".html")[0]
        event_data = {
            "id" :id,
            "date": child_tds[3].text,
            "lat": child_tds[5].text,
            "long": child_tds[7].text,
            "depth": child_tds[9].text.split(" ")[0],
            "magnitude": child_tds[11].text.split(" ")[0],
        }
        return Event(**event_data)  

def parse_json_event(str_event: str) -> Event:
    ev = json.loads(str_event)
    return Event(ev["id"], ev["utc"], ev["latitude"], ev["longitude"], ev["depth"], ev["magnitude"])
