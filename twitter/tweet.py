import asyncio

from core.abstract_item import AbstractItem


class Tweet(AbstractItem):
    def __init__(self, id: int, date: str, author: str, message: str):
        self.id = id
        self.date = date
        self.author = author
        self.message = message
        super().__init__()

    def get_raw_data(self) -> bytes:
        return (self.date + str(self.id) + self.author + self.message).encode()

    def get_marker(self) -> str:
        return self.date
