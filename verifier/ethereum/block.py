import datetime
import hashlib


class Block:
    def __init__(self, number: int, hashes):
        self.number = number
        self.hashes = set()
        self.hashes.update(hashes)

    def __eq__(self, other):
        return self.number == other.number

    def get_marker(self) -> str:
        return self.number

    def __str__(self) -> str:
        return f"Block<number={self.number},hashes={self.hashes}>"
