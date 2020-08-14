import logging
from collections import OrderedDict
from typing import List

from radio.frame import Frame

log = logging.getLogger(__name__)


class Buffer:
    def __init__(self, size: int, prefix: str):
        self.buffer = OrderedDict()
        self.prefix = prefix
        self.size = size
        self.possible = 0

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Frame) -> None:
        self.buffer[item.get_marker()] = item
        if item.get_marker()[:len(self.prefix)] == self.prefix:
            self.possible += 1
        if len(self.buffer) > self.size:
            popped = self.buffer.popitem(False)
            if popped.get_marker()[:len(self.prefix)] == self.prefix:
                self.possible -= 1


    def check_marker(self, marker: str) -> bool:
        log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        i = 0
        if marker in self.buffer:
            while len(self.buffer) > 0:
                k, v = self.buffer.popitem(False)
                if k == marker:
                    self.buffer[k] = v
                    self.buffer.move_to_end(k, False)
                    log.debug(f"removed {i} elements before hash...")
                    return True
                if v.get_marker()[:len(self.prefix)] == self.prefix:
                    self.possible -= 1
                i += 1
        log.debug(f"marker {marker} not found...")
        return False

    def get_list(self, size: int) -> List[Frame]:
        if len(self.buffer) < size:
            log.debug(f"buffer not full yet ({len(self.buffer)}/{self.size}), try again in a few seconds...")
            return []
        else:
            res: List[Frame] = []
            for _ in range(size):
                _, v = self.buffer.popitem(False)
                if v.get_marker()[:len(self.prefix)] == self.prefix:
                    self.possible -= 1
                res.append(v)
            return res
