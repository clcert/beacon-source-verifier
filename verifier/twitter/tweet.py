import datetime


class Tweet:

    def __init__(self, id: int, created_date: str, author: str, message: str):
        self.id = id
        self.datestr = created_date
        self.date = datetime.datetime.fromisoformat(created_date[:-1])
        self.author = author
        self.message = message

    def get_canonical_form(self) -> bytes:
        return (self.datestr + str(self.id) + self.author + self.message).encode()

    def get_tuple(self):
        return self.datestr, self.id, self.author, self.message

    def __eq__(self, other):
        return self.get_tuple() == other.get_tuple()

    def __gt__(self, other):
        return self.id > other.id

    def __str__(self) -> str:
        return f"Tweet<id={self.id},date={self.datestr},author={self.author},message={self.message}>"
