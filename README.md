# Globally Unique Incremental ID Generator (Python + AWS DynamoDB)

This project implements a globally unique, incremental ID generator with sequential range allocation using Amazon DynamoDB atomic counters. It includes unit tests, a heavy load test, and an optional FastAPI REST layer.

## Design

- Global uniqueness: DynamoDB `UpdateItem` with `ADD` ensures atomic increments across all clients.
- Sequential ranges: Atomically add `count`, compute start = `new - count + 1` and return `[start..new]`.
- Thread-safety: Local lock prevents duplicate local work; DynamoDB provides global atomicity.

## Project layout

- `src/id_generator.py`: Abstract interface
- `src/dynamodb_id_generator.py`: DynamoDB-backed implementation
- `src/api.py`: FastAPI REST API
- `tests/`: Unit and load tests (using `moto`)

## Requirements

- Python 3.10+

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run tests

```bash
pytest -q
```

## Load test only

```bash
pytest -q tests/test_load.py -q
```

## Run REST API (backed by real AWS or DynamoDB Local)

By default, `src/api.py` will auto-create the table if missing.

- Using AWS credentials (real DynamoDB):

```bash
export AWS_REGION=us-east-1
uvicorn src.api:app --host 0.0.0.0 --port 8000
```

- Using DynamoDB Local:

```bash
# Start DynamoDB Local
docker run -p 8000:8000 amazon/dynamodb-local:2.5.2 -jar DynamoDBLocal.jar -sharedDb

# Run API pointing to local endpoint
export AWS_REGION=us-east-1
uvicorn 'src.api:create_app(table_name="id_counters", counter_id="global", region_name="us-east-1", endpoint_url="http://localhost:8000")' --factory --host 0.0.0.0 --port 8001
```

Test endpoints:

```bash
curl http://65.2.171.2:8000/next
curl "http://65.2.171.2:8001/range?count=10"
```

## Notes

- The tests use `moto` to mock DynamoDB. For cross-process testing, prefer DynamoDB Local or a real table.
- To optimize latency at extreme QPS, you can extend the implementation to allocate local blocks (e.g., fetch 100 IDs at once) and serve from memory. The current version performs a single DynamoDB call per `next_id` or `get_id_range` invocation. 
