import asyncio
import logging
from abc import abstractmethod, ABCMeta
from threading import Thread, Event
import time
from core.source_manager import SourceManager

from typing import List

log = logging.getLogger(__name__)


class AbstractSource(metaclass=ABCMeta):
    """
    Represents an abstract event source
    """
    NAME = "abstract_source"
    ID = 0
    RESTART_TIME = 5

    def __init__(self, mgr: SourceManager):
        self.manager = mgr
        self.stop_event = Event()
        self.loop = asyncio.new_event_loop()
        self.thread: Thread = Thread(target=self.run_loop)

    def run_loop(self) -> None:
        """
        Executes this collector indefinitely
        :return:
        """
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def run_collector(self) -> None:
        """
        Executes asynchronous collection of events from the source
        """
        self.manager.metrics.collector_status.labels(self.name()).state('running')
        while True:
            log.info(f"Starting {self.name()} collector...")
            try:
                await self.init_collector()
                while not self.stop_event.is_set():
                    await self.collect()
                await self.finish_collector()
                return
            except Exception as e:
                self.manager.metrics.exceptions_number.labels(self.name).inc(1)
                log.error(f"Exception in {self.name()} collector: {e.__str__()}, restarting in {AbstractSource.RESTART_TIME} seconds...")
                time.sleep(AbstractSource.RESTART_TIME)

    async def stop_collector(self):
        """
        Sends a stop signal to the collector running asynchronously.
        :return:
        """
        self.stop_event.set()
        self.stop_event.clear()

    def name(self) -> str:
        """
        Returns source name
        :return: source name
        """
        return self.NAME
        
    @abstractmethod
    async def verify(self, params: map) -> map:
        """
        Verifies a pulse using buffer data and pulse metadata.
        :param params: Pulse metadata
        :return: A map with the name of the source and its verification value (True, False)
        """
        pass

    @abstractmethod
    async def init_collector(self) -> None:
        """
        Initializes the collector, preparing it to run.
        :return:
        """
        pass

    @abstractmethod
    async def collect(self) -> None:
        """
        Runs the collector
        :return:
        """
        pass

    @abstractmethod
    async def finish_collector(self) -> None:
        """
        Stops the collector
        :return:
        """
        pass

    @abstractmethod
    async def get_possible(self) -> List[str]:
        """
        Returns a list of possible metadatas
        :return a list of possible metadatas:
        """
        pass
