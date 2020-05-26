import asyncio

from core.abstract_item import AbstractItem


class Tweet(AbstractItem):
    FIELD_ORDER = ("created_at", "id", "author_id", "text")

    def __init__(self, date: str, id: int, author: str, message: str):
        self.date = date
        self.id = id
        self.author = author
        self.message = message
        super().__init__()

    def get_raw_data(self) -> bytes:
        return (self.date + str(self.id) + self.author + self.message).encode()

    def get_marker(self) -> str:
        return self.date
