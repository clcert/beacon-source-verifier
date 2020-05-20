import asyncio
import logging
from datetime import datetime
from threading import Thread
from typing import List, Set

import requests

from core.abstract_source import AbstractSource


class SourceManager:

    def __init__(self, verifier_api: str,
                 verification_interval: int = 60,
                 verify_timeout: int = 30,
                 stop_collector_timeout: int = 10):
        self.collector_futures: Set[asyncio.Future] = set()
        self.sources: List[AbstractSource] = []
        self.verify_timeout = verify_timeout
        self.stop_collector_timeout = stop_collector_timeout
        self.verifier_api = verifier_api
        self.verification_interval = verification_interval
        self.collector_loop = asyncio.new_event_loop()
        self.collector_thread = Thread(target=self.run_loop)
        self.collector_thread.start()

    def run_loop(self):
        asyncio.set_event_loop(self.collector_loop)
        self.collector_loop.run_forever()

    def add_source(self, source: AbstractSource) -> None:
        """
        Registers a source
        """
        self.sources.append(source)

    def start_collection(self):
        logging.debug(f"Starting collectors: {[source.name() for source in self.sources]}")
        self.collector_futures.update(
            [asyncio.run_coroutine_threadsafe(source.run_collector(), self.collector_loop) for source in
             self.sources])

    async def stop_collection(self):
        logging.debug(f"Stopping collectors: {[source.name() for source in self.sources]}")
        for source in self.sources:
            await source.stop_collector()
        done, pending = await asyncio.wait({future for future in self.collector_futures},
                                           return_when=asyncio.FIRST_EXCEPTION,
                                           timeout=self.stop_collector_timeout)
        for p in pending:
            p.cancel()

    async def run_verification(self):
        logging.debug("Starting verification process...")
        while True:
            start_time = datetime.now()
            res = await self.run_one_verification()
            print(res)
            end_time = datetime.now()
            wait_time = 60 - (end_time - start_time).seconds
            logging.debug(f"finished this cycle, waiting {wait_time} seconds")
            await asyncio.sleep(wait_time)

    async def run_one_verification(self) -> map:
        params = self.get_params()
        done, pending = await asyncio.wait(
            {asyncio.create_task(source.verify(params[source.id()])) for source in self.sources},
            timeout=self.verify_timeout)
        for task in pending:
            task.cancel()
        joined_map = {}
        for res in done:
            joined_map.update(res.result())
        return joined_map

    def get_params(self) -> map:
        pulse = requests.get(f"{self.verifier_api}/pulse/last").json()
        extValues = requests.get(f"{self.verifier_api}/extValue/{pulse['pulse']['external']['value']}").json()[
            "eventsCollected"]
        paramsMap = {}
        for value in extValues:
            paramsMap[value["sourceId"]] = value
        return paramsMap
