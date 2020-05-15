from abc import ABC, abstractmethod


class AbstractItem(ABC):

    def __init__(self, data: any):
        self.data = data

    @abstractmethod
    def get_raw_data(self) -> str:
        """
        Returns raw data of the item as a bytes object
        :return:
        """
        pass

    @abstractmethod
    def get_hash(self) -> str:
        """
        Returns the hash of the item
        :return: hash of the item
        """
        pass
