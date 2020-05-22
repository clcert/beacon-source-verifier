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
        return (str(self.id) + self.date + self.author + self.message).decode()

    def get_marker(self) -> str:
        return id

    async def read(self, reader: asyncio.StreamReader):
        await self.header.read(reader)
        to_read = self.header.body_size
        while to_read != 0:
            new_data = await reader.read(to_read)
            to_read -= len(new_data)
            self.data += new_data

