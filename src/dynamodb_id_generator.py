import threading
from decimal import Decimal
from typing import List, Optional

import boto3
from botocore.client import Config

from .id_generator import IdGenerator


class DynamoDbIdGenerator(IdGenerator):
	"""
	ID generator backed by Amazon DynamoDB atomic counters.

	- Global uniqueness across processes and servers via DynamoDB's atomic UpdateItem with ADD.
	- Sequential ranges by atomically adding `count` and computing the start = new_value - count + 1.
	- Thread-safe for local instance; the remote operation is atomic, and local lock avoids duplicate local work.
	"""

	def __init__(
		self,
		table_name: str,
		counter_id: str = "global",
		region_name: str = 'ap-south-1',
        endpoint_url: Optional[str] = None,
		boto3_resource: Optional[object] = None,
		create_table_if_not_exists: bool = False,
	):
		self._table_name = table_name
		self._counter_id = counter_id
		self._lock = threading.Lock()
		print(region_name)
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
		# Wait for existence
		self._dynamodb.Table(self._table_name).wait_until_exists()

	def next_id(self) -> int:
		with self._lock:
			return self._add_and_get_new_value(1)

	def get_id_range(self, count: int) -> List[int]:
		if count <= 0:
			raise ValueError("count must be a positive integer")
		with self._lock:
			new_value = self._add_and_get_new_value(count)
			start = new_value - count + 1
			return list(range(start, new_value + 1))

	def _add_and_get_new_value(self, count: int) -> int:
		response = self._table.update_item(
			Key={"counter_id": self._counter_id},
			UpdateExpression="ADD #v :inc",
			ExpressionAttributeNames={"#v": "value"},
			ExpressionAttributeValues={":inc": Decimal(count)},
			ReturnValues="UPDATED_NEW",
		)
		value = response["Attributes"]["value"]
		# DynamoDB returns Decimal; convert to int
		return int(value) 
