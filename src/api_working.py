from typing import List, Optional

import boto3
from fastapi import FastAPI, HTTPException, Query

from .dynamodb_id_generator import DynamoDbIdGenerator


def create_app(
	table_name: str,
	counter_id: str = "global",
	region_name: Optional[str] = None,
	endpoint_url: Optional[str] = None,
):
	app = FastAPI(title="ID Generator API", version="1.0.0")

	gen = DynamoDbIdGenerator(
		table_name=table_name,
		counter_id=counter_id,
		region_name=region_name,
		endpoint_url=endpoint_url,
		create_table_if_not_exists=True,
	)

	@app.get("/next")
	def get_next() -> int:
		return gen.next_id()

	@app.get("/range")
	def get_range(count: int = Query(1, gt=0, le=100000)) -> List[int]:
		try:
			return gen.get_id_range(count)
		except ValueError as e:
			raise HTTPException(status_code=400, detail=str(e))

	return app


# Default app for uvicorn: `uvicorn src.api:app --reload`
app = create_app(table_name="id_counters", counter_id="global") 