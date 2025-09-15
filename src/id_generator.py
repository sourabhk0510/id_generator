from abc import ABC, abstractmethod
from typing import List


class IdGenerator(ABC):
	@abstractmethod
	def next_id(self) -> int:
		"""Get the next ID number (globally unique and incremental)."""
		raise NotImplementedError

	@abstractmethod
	def get_id_range(self, count: int) -> List[int]:
		"""Get a range of sequential ID numbers of size `count`."""
		raise NotImplementedError 