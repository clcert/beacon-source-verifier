from datetime import time
from threading import Thread
from typing import List

from core.abstract_source import AbstractSource

BEACON_VERIFIER_API = "https://random.uchile.cl/beacon/2.0/pulse/time/{}"

class SourceManager:
    def __init__(self, timeout: int = 30):
        self.sources: List[AbstractSource] = []
        self.collect_threads: List[Thread] = []
        self.verify_threads: List[Thread] = []
        self.timeout = timeout

    def add_source(self, source: AbstractSource) -> None:
        """
        Registers a source
        """
        self.sources.append(source)

    def start_collection(self):
        for source in self.sources:
            # Run collector in a thread
            self.collect_threads.append(Thread(None, source.run_collector))
            self.collect_threads[-1].run()

    def run_verification(self):
        while True:
            # get pulse

            # Get verification daa
            current_time = int(time.time()) * 1000
            map = {}
            for source in self.sources:
                self.collect_threads.append(Thread(None, source.verify_data, map[source.name()]))
        self.verify_threads = []
