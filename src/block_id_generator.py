import threading
from decimal import Decimal
from typing import List, Optional

import boto3
from botocore.client import Config

from .id_generator import IdGenerator


class BlockAllocatingDynamoDbIdGenerator(IdGenerator):
	"""
	Prefetches blocks of IDs from DynamoDB and serves `next_id()` from memory.

	- Global uniqueness via DynamoDB's atomic increment.
	- `next_id()` uses local block for low latency; auto-refills when exhausted.
	- `get_id_range(count)` consumes from local block first, then reserves more as needed,
	  guaranteeing strict sequentiality across calls without gaps.
	- Thread-safe within process; works across processes/hosts.
	"""

	def __init__(
		self,
		table_name: str,
		counter_id: str = "global",
		block_size: int = 100,
		region_name: Optional[str] = None,
		endpoint_url: Optional[str] = None,
		boto3_resource: Optional[object] = None,
		create_table_if_not_exists: bool = False,
	):
		if block_size <= 0:
			raise ValueError("block_size must be positive")
		self._table_name = table_name
		self._counter_id = counter_id
		self._block_size = block_size

		self._lock = threading.Lock()
		self._local_next: Optional[int] = None
		self._local_end: Optional[int] = None

		if boto3_resource is not None:
			self._dynamodb = boto3_resource
		else:
			self._dynamodb = boto3.resource(
				"dynamodb",
				"ap-south-1",
				endpoint_url=endpoint_url,
				config=Config(retries={"max_attempts": 10, "mode": "standard"}),
			)

		if create_table_if_not_exists:
			self._ensure_table()

		self._table = self._dynamodb.Table(self._table_name)

	def _ensure_table(self) -> None:
		existing_tables = [t.name for t in self._dynamodb.tables.all()]
		if self._table_name in existing_tables:
			return
		self._dynamodb.create_table(
			TableName=self._table_name,
			AttributeDefinitions=[{"AttributeName": "counter_id", "AttributeType": "S"}],
			KeySchema=[{"AttributeName": "counter_id", "KeyType": "HASH"}],
			BillingMode="PAY_PER_REQUEST",
		)
		self._dynamodb.Table(self._table_name).wait_until_exists()

	def _atomic_add(self, count: int) -> int:
		response = self._table.update_item(
			Key={"counter_id": self._counter_id},
			UpdateExpression="ADD #v :inc",
			ExpressionAttributeNames={"#v": "value"},
			ExpressionAttributeValues={":inc": Decimal(count)},
			ReturnValues="UPDATED_NEW",
		)
		return int(response["Attributes"]["value"])  # new value after add

	def _reserve_block(self, size: int) -> None:
		new_value = self._atomic_add(size)
		start = new_value - size + 1
		self._local_next = start
		self._local_end = new_value

	def next_id(self) -> int:
		with self._lock:
			if self._local_next is None or self._local_end is None or self._local_next > self._local_end:
				self._reserve_block(self._block_size)
			val = self._local_next
			self._local_next += 1
			return val  # type: ignore[return-value]

	def get_id_range(self, count: int) -> List[int]:
		if count <= 0:
			raise ValueError("count must be a positive integer")
		with self._lock:
			result: List[int] = []
			remaining = count
			while remaining > 0:
				# If local block empty, reserve a new block sized for efficiency
				if self._local_next is None or self._local_end is None or self._local_next > self._local_end:
					# Reserve at least remaining, but we can over-reserve by block_size for future calls
					reserve_size = max(self._block_size, remaining)
					self._reserve_block(reserve_size)
				# Consume from local block
				available = self._local_end - self._local_next + 1  # type: ignore[operator]
				take = min(remaining, available)
				start = self._local_next  # type: ignore[assignment]
				end = start + take - 1
				result.extend(range(start, end + 1))
				self._local_next = end + 1
				remaining -= take
			return result 
