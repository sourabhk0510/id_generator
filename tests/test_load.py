import threading
from concurrent.futures import ThreadPoolExecutor

import boto3
from moto import mock_aws

from src.dynamodb_id_generator import DynamoDbIdGenerator


def _create_table(dynamodb, table_name: str):
	dynamodb.create_table(
		TableName=table_name,
		AttributeDefinitions=[{"AttributeName": "counter_id", "AttributeType": "S"}],
		KeySchema=[{"AttributeName": "counter_id", "KeyType": "HASH"}],
		BillingMode="PAY_PER_REQUEST",
	)
	dynamodb.Table(table_name).wait_until_exists()


@mock_aws
def test_heavy_concurrent_mixed_workload():
	"""
	Simulate heavy concurrent access with a mix of single next_id and ranged allocations.
	Ensures global uniqueness and overall contiguity.
	"""
	dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
	table_name = "id_counters"
	_create_table(dynamodb, table_name)
	gen = DynamoDbIdGenerator(table_name=table_name, counter_id="heavy", boto3_resource=dynamodb)

	total_single = 5000
	# total allocated by ranges
	ranges = [2, 3, 5, 7, 10, 20, 50]
	repeat_ranges = 200

	results = []
	lock = threading.Lock()

	def do_single():
		val = gen.next_id()
		with lock:
			results.append(val)

	def do_range(k: int):
		vals = gen.get_id_range(k)
		with lock:
			results.extend(vals)

	with ThreadPoolExecutor(max_workers=128) as ex:
		for _ in range(total_single):
			ex.submit(do_single)
		for _ in range(repeat_ranges):
			for k in ranges:
				ex.submit(do_range, k)

	# Validate
	assert len(results) == total_single + repeat_ranges * sum(ranges)
	assert len(set(results)) == len(results)
	mn, mx = min(results), max(results)
	assert set(results) == set(range(mn, mx + 1)) 