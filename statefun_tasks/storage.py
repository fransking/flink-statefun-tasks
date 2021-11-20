from abc import ABC, abstractmethod
from typing import List


class StorageBackend(ABC):
    def __init__(self):
        pass

    @property
    @abstractmethod
    def threshold(self) -> int:
        """
        Threshold in bytes at which point this storage backend is used in place of pipeline state
        """
        pass

    @abstractmethod
    async def store(self, keys: List[str], value: bytes):
        pass

    @abstractmethod
    async def fetch(self, keys: List[str]) -> bytes:
        pass

    @abstractmethod
    async def delete(self, keys: List[str]):
        pass
