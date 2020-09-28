import logging
import heapq
from collections import OrderedDict
from typing import List, Set

from ethereum.block import Block
from prometheus_client import Gauge

log = logging.getLogger(__name__)


class Buffer:
    def __init__(self, metric: Gauge, size: int):
        self.buffer = OrderedDict()
        self.size = size
        self.metric = metric

    def __len__(self):
        return len(self.buffer)

    def total_hashes(self) -> int:
        i = 0
        for b in self.buffer.values():
            i += len(b.hashes)
        return i

    def hashes_set(self) -> Set[str]:
        s = set()
        for b in self.buffer.values():
            s = s.union(b.hashes)
        return s
    
    def add(self, item: Block) -> None:
        if item.get_marker() in self.buffer:
            self.buffer[item.get_marker()].hashes.update(item.hashes)
        else:
            self.buffer[item.get_marker()] = item   
        if len(self.buffer) > self.size:
            self.buffer.popitem(False)
        self.metric.observe(len(self.buffer))

    def check_marker(self, marker: str) -> bool:
        log.debug(f"checking marker {marker} (buffer size = {len(self.buffer)} items)")
        i = 0
        res = False
        if marker in self.buffer:
            while len(self.buffer) > 0:
                k, v = self.buffer.popitem(False)
                if k == marker:
                    self.buffer[k] = v
                    self.buffer.move_to_end(k, False)
                    log.debug(f"removed {i} elements before marker...")
                    res = True
                    break
                i += 1
        self.metric.observe(len(self.buffer))
        return res

    def get_first(self) -> Block:
        k, v = self.buffer.popitem(False)
        self.buffer[k] = v
        self.buffer.move_to_end(k, False)
        self.metric.observe(len(self.buffer))
        return v

    def __str__(self) -> str:
        result = []
        for k, v in self.buffer.items():
            result.append(f"{k}={v}")
        return f"EthBuffer<{','.join(result)}>"
