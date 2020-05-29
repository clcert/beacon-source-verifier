import asyncio
import logging
from abc import abstractmethod, ABCMeta
from threading import Thread, Event


class AbstractSource(metaclass=ABCMeta):
    """
    Represents an abstract event source
    """
    NAME = "abstract_source"
    ID = 0

    def __init__(self):
        self.stop_event = Event()
        self.loop = asyncio.new_event_loop()
        self.thread: Thread = Thread(target=self.run_loop)

    def run_loop(self) -> None:
        """
        Executes this collector indefinitely
        :return:
        """
        logging.info(f"Starting {self.name()} loop...")
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def run_collector(self) -> None:
        """
        Executes asynchronous collection of events from the source
        """
        try:
            await self.init_collector()
            while not self.stop_event.is_set():
                await self.collect()
            await self.finish_collector()
        except Exception as e:
            print(f"Exception in {self.name()} collector: {e.__str__()}")

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

    def id(self) -> int:
        """
        Returns source id
        :return: source id
        """
        return self.ID

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
