import asyncio
import logging
from datetime import datetime
from threading import Thread
from typing import List, Set

import requests

from core.abstract_source import AbstractSource


class BeaconAPIException(Exception):
    pass


class SourceManager:
    """
    SourceManager groups, starts and stops a set of sources.
    """

    def __init__(self, config: map):
        self.collector_futures: Set[asyncio.Future] = set()
        self.sources: List[AbstractSource] = []
        self.verification_timeout = config["verification_timeout"]
        self.collector_stop_timeout = config["collector_stop_timeout"]
        self.base_api = config["base_api"]
        self.verification_interval = config["verification_interval"]

    def add_source(self, source: AbstractSource) -> None:
        """
        Registers a source into the collector
        """
        self.sources.append(source)


    def start_collection(self) -> None:
        """
        Starts collection of events indefinitely
        :return:
        """
        logging.debug(f"Starting collectors: {[source.name() for source in self.sources]}")
        threads = [source.thread.start() for source in self.sources]
        self.collector_futures.update(
            [asyncio.run_coroutine_threadsafe(source.run_collector(), source.loop) for source in self.sources])

    async def stop_collection(self) -> None:
        """
        Stops the collection of events waiting at most stop_collector_timeout seconds
        :return:
        """
        logging.debug(f"Stopping collectors: {[source.name() for source in self.sources]}")
        for source in self.sources:
            await source.stop_collector()
        done, pending = await asyncio.wait({future for future in self.collector_futures},
                                           return_when=asyncio.FIRST_EXCEPTION,
                                           timeout=self.collector_stop_timeout)
        for p in pending:
            p.cancel()

    async def run_verification(self):
        """
        Thread that executes the verifications of pulses.
        :return:
        """
        await asyncio.sleep(2 * self.verification_interval)
        logging.debug("Starting verification process...")
        while True:
            try:
                start_time = datetime.now()
                res = await self.run_one_verification()
                print(res)
                end_time = datetime.now()
                wait_time = 60 - (end_time - start_time).seconds
                logging.debug(f"finished this cycle, waiting {wait_time} seconds")
                await asyncio.sleep(wait_time)
            except Exception as e:
                logging.error(f"exception verifying pulse: {e}")

    async def run_one_verification(self) -> map:
        """
        Verifies a single pulse with all the enabled source verifiers.
        :return: Map with name of source as key, and source status as value
        """
        params = self.get_params()
        done, pending = await asyncio.wait(
            {asyncio.create_task(source.verify(params[source.name()])) for source in self.sources},
            timeout=self.verification_timeout)
        for task in pending:
            task.cancel()
        joined_map = {}
        for res in done:
            joined_map.update(res.result())
        print(joined_map)
        return joined_map

    def get_params(self) -> map:
        """
        Returns a map with the verification params of the current pulse.
        Each param is tagged with the respective source ID.
        :return: map with params
        """
        pulse_req = requests.get(f"{self.base_api}/pulse/last")
        if pulse_req.status_code != 200:
            raise BeaconAPIException()
        pulse = pulse_req.json()
        extValues_req = requests.get(f"{self.base_api}/extValue/{pulse['pulse']['external']['value']}")
        if extValues_req.status_code != 200:
            raise BeaconAPIException()
        extValues = extValues_req.json()["eventsCollected"]
        paramsMap = {}
        for value in extValues:
            paramsMap[value["sourceName"]] = value
        return paramsMap
