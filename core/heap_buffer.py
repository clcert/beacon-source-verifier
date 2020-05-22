import heapq
import logging
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import List

from core.abstract_item import AbstractItem
from core.buffer import Buffer


class HeapBuffer(Buffer):
    def __init__(self, size: int, heapifyfunc):
        self.buffer = []
        self.heapify_function = heapifyfunc
        super().__init__(size)

    def __len__(self):
        return len(self.buffer)

    def add(self, item: AbstractItem) -> None:
        if len(self.buffer) == self.size:
            heapq.heapreplace(self.buffer, self.heapify_function(item))
        else:
            heapq.heappush(self.buffer, self.heapify_function(item))

    def check_marker(self, marker: str) -> bool:
        logging.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        while len(self.buffer) > 0:
            item = heapq.heappop(self.buffer)[-1]
            if item.get_marker() == marker:
                heapq.heappush(self.buffer, item)
                return True
        return False

    def get_list(self, size: int) -> List[AbstractItem]:
        items = []
        if len(items) < size:
            return []
        item = heapq.heappop(self.buffer)[-1]
        marker = item.get_marker()
        while item.get_marker() == marker:
            items.append(item)
        return items
