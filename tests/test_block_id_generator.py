import os
import boto3
from src.dynamodb_id_generator import DynamoDbIdGenerator
from src.block_id_generator import BlockAllocatingDynamoDbIdGenerator

def test_real_dynamodb_next_and_range():
    region = os.environ.get("AWS_REGION", "ap-south-1")
    table = os.environ.get("TABLE_NAME", "id_counters")
    counter = os.environ.get("COUNTER_ID", "integration")

    ddb = boto3.resource("dynamodb", region_name=region)
    # ensure counter item exists once
    tbl = ddb.Table(table)
    tbl.put_item(Item={"counter_id": counter, "value": 0})

    gen = DynamoDbIdGenerator(table_name=table, counter_id=counter, region_name=region)
    print(gen)
    id1 = gen.next_id()
    print(id1)
    id2 = gen.next_id()
    print(id2)
    assert id2 == id1 + 1

    r = gen.get_id_range(5)
    assert r == list(range(r[0], r[0] + 5))

def test_real_dynamodb_block_allocator():
    region = os.environ.get("AWS_REGION", "ap-south-1")
    table = os.environ.get("TABLE_NAME", "id_counters")
    counter = os.environ.get("COUNTER_ID", "global")

    ddb = boto3.resource("dynamodb", region_name=region)
    tbl = ddb.Table(table)
    tbl.put_item(Item={"counter_id": counter, "value": 100})

    gen = BlockAllocatingDynamoDbIdGenerator(table_name=table, counter_id=counter, region_name=region, block_size=16)
    ids = [gen.next_id() for _ in range(10)]
    assert ids == list(range(ids[0], ids[0] + 10))
    rng = gen.get_id_range(6)
    print(rng)
    assert rng == list(range(ids[-1] + 1, ids[-1] + 7))
