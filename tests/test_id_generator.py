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
def test_next_id_sequential_single_instance():
	dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
	table_name = "id_counters"
	#_create_table(dynamodb, table_name)
	gen = DynamoDbIdGenerator(table_name=table_name, counter_id="test", boto3_resource=dynamodb)

	id1 = gen.next_id()
	id2 = gen.next_id()
	assert id2 == id1 + 1


@mock_aws
def test_get_id_range_returns_sequential_range():
	dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
	table_name = "id_counters"
	#_create_table(dynamodb, table_name)
	gen = DynamoDbIdGenerator(table_name=table_name, counter_id="range", boto3_resource=dynamodb)

	range1 = gen.get_id_range(100)
	print(range1)
	assert len(range1) == 100
	# Must be strictly sequential
	assert range1 == list(range(range1[0], range1[0] + 100))

	# Next call should pick up exactly after the previous
	range2 = gen.get_id_range(3)
	print(range2)
	assert range2 == [range1[-1] + 1, range1[-1] + 2, range1[-1] + 3]


@mock_aws
def test_concurrent_next_id_no_duplicates_and_contiguous():
	dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
	table_name = "id_counters"
	#_create_table(dynamodb, table_name)
	gen = DynamoDbIdGenerator(table_name=table_name, counter_id="load", boto3_resource=dynamodb)

	n = 1000
	results = []
	lock = threading.Lock()

	def work():
		val = gen.next_id()
		with lock:
			results.append(val)

	with ThreadPoolExecutor(max_workers=64) as ex:
		for _ in range(n):
			ex.submit(work)

	assert len(results) == n
	assert len(set(results)) == n  # uniqueness
	mn, mx = min(results), max(results)
	# Should be exactly contiguous range with no gaps
	print(results)
	assert set(results) == set(range(mn, mx + 1)) 
