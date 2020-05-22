import logging
from abc import ABC, abstractmethod

class AbstractItem(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def get_raw_data(self) -> bytes:
        """
        Returns raw data of the item as a bytes object
        :return:
        """
        pass

    @abstractmethod
    def get_marker(self) -> str:
        """
        Returns the hash of the item
        :return: hash of the item
        """
        pass
