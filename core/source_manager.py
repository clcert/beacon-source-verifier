import asyncio
import json
import logging
import os
from datetime import datetime
from typing import List, Set

import requests

from core.abstract_source import AbstractSource

log = logging.getLogger(__name__)


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
        self.output_path = config["output_folder"]
        self.threads = None
        os.makedirs(self.output_path, exist_ok=True)

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
        log.debug(f"Starting collectors: {[source.name() for source in self.sources]}")
        self.threads = [source.thread.start() for source in self.sources]
        self.collector_futures.update(
            [asyncio.run_coroutine_threadsafe(source.run_collector(), source.loop) for source in self.sources])

    async def stop_collection(self) -> None:
        """
        Stops the collection of events waiting at most stop_collector_timeout seconds
        :return:
        """
        log.debug(f"Stopping collectors: {[source.name() for source in self.sources]}")
        for source in self.sources:
            await source.stop_collector()
        _, pending = await asyncio.wait({future for future in self.collector_futures},
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
        log.debug("Starting verification process...")
        while True:
            start_time = datetime.now()
            try:
                await self.run_one_verification()
            except Exception as e:
                log.error(f"exception verifying pulse: {e}")
            end_time = datetime.now()
            wait_time = 60 - (end_time - start_time).seconds
            log.debug(f"finished this cycle, waiting {wait_time} seconds")
            await asyncio.sleep(wait_time)

    async def run_one_verification(self):
        """
        Verifies a single pulse with all the enabled source verifiers.
        """
        pulseID, params = self.get_params()
        done, pending = await asyncio.wait(
            {asyncio.create_task(source.verify(params[source.name()])) for source in self.sources},
            timeout=self.verification_timeout)
        for task in pending:
            task.cancel()
        joined_map = {}
        for res in done:
            joined_map.update(res.result())
        self.save_response(pulseID, joined_map)

    def get_params(self) -> map:
        """
        Returns a map with the verification params of the current pulse.
        Each param is tagged with the respective source ID.
        :return: pulse id and map with params
        """
        pulse_req = requests.get(f"{self.base_api}/pulse/last")
        if pulse_req.status_code != 200:
            raise BeaconAPIException(f"Pulse API answered with non-200 code: {pulse_req.status_code}")
        pulse = pulse_req.json()
        extValues_req = requests.get(f"{self.base_api}/extValue/{pulse['pulse']['external']['value']}")
        if extValues_req.status_code != 200:
            raise BeaconAPIException(f"ExtValue API answered with non-200 code: {extValues_req.status_code}")
        extValues = extValues_req.json()["eventsCollected"]
        paramsMap = {}
        for value in extValues:
            paramsMap[value["sourceName"]] = value
        return pulse["pulse"]["uri"], paramsMap

    def save_response(self, pulse, sources):
        response = {
            "pulse": pulse,
            "valid": True,
            "checked_date": datetime.now().isoformat(),
            "sources": sources,
        }
        log.info(json.dumps(response))
        pulse_splitted = pulse.split("/")[-4:]
        folder = f"{self.output_path}{'/'.join(pulse_splitted[:3])}"
        os.makedirs(folder, exist_ok=True)
        with open(f"{folder}/{pulse_splitted[3]}.json", 'w') as f:
            json.dump(response, f)
        return response
