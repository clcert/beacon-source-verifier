import datetime


class Tweet:
    def __init__(self, id: int, created_date: str, author: str, message: str):
        self.id = id
        self.datestr = created_date
        self.date = datetime.datetime.strptime(created_date, "%a %b %d %H:%M:%S +0000 %Y")
        self.author = author
        self.message = message

    def get_raw_data(self) -> bytes:
        return (self.datestr + str(self.id) + self.author + self.message).encode()

    def get_tuple(self):
        return self.datestr, self.id, self.author, self.message

    def __eq__(self, other):
        return self.get_tuple() == other.get_tuple()
