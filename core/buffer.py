import logging
from abc import ABC, abstractmethod
from typing import List

from core.abstract_item import AbstractItem


class Buffer(ABC):

    def __init__(self, size: int):
        self.size = size
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def add(self, item: AbstractItem) -> None:
        """
        Adds an item to the buffer. If the buffer is full it drops the oldest item
        :param item:
        :return:
        """
        pass

    @abstractmethod
    def check_marker(self, marker: str) -> bool:
        """
        Checks if hash exist in buffer and if it exists, returns a list of return_size elements.
        :param marker: hash to look for
        :param return_size: number of elements to return from buffer
        :return:
        """
        pass

    @abstractmethod
    def get_list(self, size: int) -> List[AbstractItem]:
        """
        Returns a list with size elements, removed from buffer.
        If the buffer has less than size elements, it returns an empty list.
        :param size: number of elements to remove
        :return: a list with 0 or more abstract items
        """
        pass
