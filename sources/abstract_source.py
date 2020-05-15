from abc import ABC, abstractmethod
from threading import Thread, Event
from buffer.buffer import Buffer


class AbstractSource(ABC):
    def __init__(self, buffer_size: int):
        self.thread: Thread = None
        self.stop_collector = Event()
        self.buffer = Buffer(buffer_size)

    def run_collector(self) -> None:
        self.__init_collector()
        while not self.stop_collector.is_set():
            self.__collect()
        self.__finish_collector()

    def stop_collector(self):
        self.stop_collector.set()
        self.thread.join()
        self.stop_collector.clear()


    def verify_data(self, timeout: int, params: map) -> bool:
        pass

    @abstractmethod
    def name(self) -> None:
        pass

    @abstractmethod
    def id(self) -> None:
        pass

    @abstractmethod
    def __init_collector(self) -> None:
        pass

    @abstractmethod
    def __collect(self) -> None:
        pass

    @abstractmethod
    def __finish_collector(self) -> None:
        pass

    @abstractmethod
    def __verify(self, params: map) -> bool:
        pass

