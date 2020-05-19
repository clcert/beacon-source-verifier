import logging
from abc import abstractmethod, ABCMeta
from threading import Thread, Event
from core.buffer import Buffer


class AbstractSource(metaclass=ABCMeta):
    def __init__(self, buffer_size: int):
        self.stop_event = Event()
        self.buffer = Buffer(buffer_size)

    async def run_collector(self) -> None:
        try:
            await self.init_collector()
            while not self.stop_event.is_set():
                await self.collect()
            await self.finish_collector()
        except Exception as e:
            print(f"Exception in {self.name()} collector: {e.__str__()}")

    async def stop_collector(self):
        self.stop_event.set()
        self.stop_event.clear()

    def verify_data(self, timeout: int, params: map) -> bool:
        pass

    @abstractmethod
    async def verify(self, params: map) -> map:
        pass

    @abstractmethod
    def name(self) -> None:
        pass

    @abstractmethod
    def id(self) -> None:
        pass

    @abstractmethod
    async def init_collector(self) -> None:
        pass

    @abstractmethod
    async def collect(self) -> None:
        pass

    @abstractmethod
    async def finish_collector(self) -> None:
        pass
