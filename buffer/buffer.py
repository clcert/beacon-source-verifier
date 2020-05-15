from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List

from buffer.abstract_item import AbstractItem


class Buffer:
    def __init__(self, size: int):
        self.buffer = OrderedDict()
        self.size = size

    def add(self, item: AbstractItem) -> None:
        """
        Adds an item to the buffer. If the buffer is full it drops the oldest item
        :param item:
        :return:
        """
        self.buffer[item.get_hash()] = item
        if len(self.buffer) > self.size:
            self.buffer.popitem(False)

    def check_hash(self, hash: str) -> bool:
        """
        Checks if hash exist in buffer and if it exists, returns a list of return_size elements.
        :param hash: hash to look for
        :param return_size: number of elements to return from buffer
        :return:
        """
        if hash in self.buffer:
            while len(self.buffer) == 0:
                k, v = self.buffer.popitem(False)
                if k == hash:
                    self.buffer[k] = v
                    self.buffer.move_to_end(k, False)
                    return True
        return False

    def get_list(self, size: int) -> List[AbstractItem]:
        """
        Returns a list with size elements, removed from buffer.
        If the buffer has less than size elements, it returns an empty list.
        :param size: number of elements to remove
        :return: a list with 0 or more abstract items
        """
        if len(self.buffer) < size:
            return []
        else:
            res: List[AbstractItem] = []
            for i in range(size):
                k, v = self.buffer.popitem(False)
                res.append(v)
            return res
