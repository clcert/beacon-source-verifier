import asyncio
import json
import logging
import os
from datetime import datetime
from typing import List, Set, Dict
from core.metrics import Metrics


import requests

from core.results import VerifierResult, PulseResult, VerifierException, PulseException

log = logging.getLogger(__name__)


class BeaconAPIException(Exception):
    pass


class SourceManager:
    """
    SourceManager groups, starts and stops a set of sources.
    """

    def __init__(self, config: Dict[str, any]):
        self.collector_futures: Set[asyncio.Future] = set()
        self.sources = []
        self.verification_timeout = config["verification_timeout"]
        self.collector_stop_timeout = config["collector_stop_timeout"]
        self.verification_interval = config.get("verification_interval", 59)
        self.base_api = config["base_api"]
        self.output_path = config.get("output_folder", "verified")
        self.threads = None
        os.makedirs(self.output_path, exist_ok=True)
        self.metrics = Metrics()
        self.metrics.start_server(config.get("metrics_port", 9345))

    def add_source(self, source) -> None:
        """
        Registers a source into the collector
        """
        self.sources.append(source)

    def start_collection(self) -> None:
        """
        Starts collection of events indefinitely
        :return:
        """
        log.info(
            f"Starting collectors: {[source.name() for source in self.sources]}")
        self.threads = []
        for source in self.sources:
            self.metrics.collector_status.labels(
                source.name()).state('starting')
            self.threads.append(source.thread.start())
        self.collector_futures.update(
            [asyncio.run_coroutine_threadsafe(source.run_collector(), source.loop) for source in self.sources])

    async def stop_collection(self) -> None:
        """
        Stops the collection of events waiting at most stop_collector_timeout seconds
        :return:
        """
        log.debug(
            f"Stopping collectors: {[source.name() for source in self.sources]}")
        for source in self.sources:
            self.metrics.collector_status.labels(
                source.name()).state('stopping')
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
        log.info("Starting verification process...")
        while True:
            start_time = datetime.now()
            try:
                await self.run_one_verification()
            except Exception as e:
                self.metrics.exceptions_number.observe(1)
                log.error(f"exception verifying pulse: {e}")
            end_time = datetime.now()
            total_time = (end_time - start_time).seconds
            wait_time = 60 - total_time
            log.debug(f"finished this cycle, waiting {wait_time} seconds")
            await asyncio.sleep(wait_time)

    async def run_one_verification(self):
        """
        Verifies a single pulse with all the enabled source verifiers.
        """
        verification_results = []
        pulse_result = PulseResult()
        pulse_id, ext_value = self.get_latest_pulse()
        pulse_result.pulse_url = pulse_id
        log.info(f"Verifying pulse {pulse_id}")
        try:
            params = self.get_params(ext_value)
            done, pending = await asyncio.wait(
                {asyncio.create_task(source.verify(
                    params[source.name()]), name=source.name()) for source in self.sources},
                timeout=self.verification_timeout)
            for task in pending:
                pending_result = VerifierResult(task.name)
                pending_result.status_code = 250
                verification_results.append(pending_result)
                task.cancel()
            for res in done:
                try:
                    result = res.result()
                    verification_results.append(result)
                except VerifierException as e:
                    self.metrics.exceptions_number.observe(1)
                    log.error(f"Error getting result from source: {e}")
                    self.metrics.verification_status.labels(
                        [res.name, res.status_code]).observe(1)
                    verification_results.append(d)
                except Exception as e:
                    self.metrics.exceptions_number.observe(1)
                    log.error(f"Unknown exception: {e}")
                    d = VerifierResult(res.name)
                    d.status_code = 299
                    d.detail = str(e)
                    verification_results.append(d)
        except Exception as e:
            self.metrics.exceptions_number.observe(1)
            error = f"Error getting params"
            log.error(f"{error}. pulse={pulse_id} error={str(e)}")
            pulse_result.add_detail(
                error,
                f"pulse={pulse_id}",
                f"error={error}")
            pulse_result.status_code = 120
        pulse_result.finish()
        self.register_metrics(pulse_result, verification_results)
        self.save_response(pulse_result, verification_results, is_last=True)

    def get_latest_pulse(self) -> map:
        """
        Returns the latest pulse id from the beacon and the external value.
        :return: latest pulse id and external value
        """
        pulse_req = requests.get(f"{self.base_api}/pulse/last")
        if pulse_req.status_code != 200:
            raise BeaconAPIException(
                f"Pulse API answered with non-200 code: {pulse_req.status_code}")
        pulse = pulse_req.json()
        return pulse["pulse"]["uri"], pulse['pulse']['external']['value']

    def get_params(self, pulse_value: str) -> map:
        """
        Returns a map with the verification params of the external value provided.
        Each param is tagged with the respective source ID.
        :return: map with params
        """
        extValues_req = requests.get(f"{self.base_api}/extValue/{pulse_value}")
        if extValues_req.status_code != 200:
            raise BeaconAPIException(
                f"ExtValue API answered with non-200 code: {extValues_req.status_code}")
        extValues = extValues_req.json()["events"]
        paramsMap = {}
        for value in extValues:
            paramsMap[value["sourceName"]] = value
        return paramsMap

    def save_response(self, pulse_result: PulseResult, verifier_results: List[VerifierResult], is_last=True) -> None:
        response = {
            "checked_date": datetime.now().isoformat(),
            "pulse": pulse_result.get_dict(),
            "sources": {}
        }
        for result in verifier_results:
            response["sources"][result.scope] = result.get_dict()
        log.info(json.dumps(response))
        folder = f"{self.output_path}/chain/{pulse_result.get_chain()}/pulse"
        os.makedirs(folder, exist_ok=True)
        with open(f"{folder}/{pulse_result.get_id()}.json", 'w') as f:
            json.dump(response, f, indent=4, sort_keys=True)
        if is_last:
            with open(f"{self.output_path}/last.json", 'w') as last_f:
                json.dump(response, last_f)
        return response

    def register_metrics(self, pulse_result: PulseResult, verifier_results: List[VerifierResult]) -> None:
        # Pulse Metrics
        self.metrics.pulse_number.labels(
            pulse_result.get_chain()).set(pulse_result.get_id())
        self.metrics.pulse_status.labels(pulse_result.status_code).observe(1)
        # General Verifier Metrics
        for verifier in verifier_results:
            self.metrics.verification_possible.labels(
                verifier.scope).observe(verifier.possible)
            for ext_val, b in verifier.to_ext_value_map().items():
                if b:
                    self.metrics.verification_ext_value_status.labels(
                        verifier.scope, ext_val).observe(1)
            self.metrics.verification_status.labels(
                verifier.scope, verifier.status_code).observe(1)
            self.metrics.verification_seconds.labels(
                verifier.scope).observe(verifier.running_time())
