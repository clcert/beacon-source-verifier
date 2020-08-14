import logging
from collections import OrderedDict
from typing import List

from ethereum.block import Block

log = logging.getLogger(__name__)


class Buffer:
    def __init__(self, size: int):
        self.buffer = OrderedDict()
        self.size = size

    def __len__(self):
        return len(self.buffer)

    def add(self, item: Block) -> None:
        if item.get_marker() in self.buffer:
            self.buffer[item.get_marker()].hashes.update(item.hashes)
        else:
            self.buffer[item.get_marker()] = item   
        if len(self.buffer) > self.size:
            self.buffer.popitem(False)

    def check_marker(self, marker: str) -> bool:
        log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        i = 0
        if marker in self.buffer:
            while len(self.buffer) > 0:
                k, v = self.buffer.popitem(False)
                if k == marker:
                    self.buffer[k] = v
                    self.buffer.move_to_end(k, False)
                    log.debug(f"removed {i} elements before marker...")
                    return True
                i += 1
        log.debug(f"marker {marker} not found...")
        return False

    def get_first(self) -> Block:
        k, v = self.buffer.popitem(False)
        self.buffer[k] = v
        self.buffer.move_to_end(k, False)
        return v

    def __str__(self) -> str:
        result = []
        for k, v in self.buffer.items():
            result.append(f"{k}={v}")
        return f"EthBuffer<{','.join(result)}>"
