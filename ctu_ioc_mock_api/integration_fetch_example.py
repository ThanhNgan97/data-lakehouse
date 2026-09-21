"""
Minimal HTTP fetch example to plug into the existing CTU IOC Bronze ingestion adapter.

This file intentionally stops at fetching + inspecting the API response.
Reuse the project's existing JSON Schema validation, explicit Spark schema,
Bronze metadata, checksum and MinIO Bronze writer after `payload` is obtained.
"""

import json
import os
from urllib.request import Request, urlopen

API_URL = os.getenv(
    "CTU_IOC_API_URL",
    "http://ctu-ioc-mock-api:8000/api/v1/education/learning-outcomes",
)
API_KEY = os.getenv("CTU_IOC_API_KEY")

headers = {"Accept": "application/json"}
if API_KEY:
    headers["Authorization"] = f"Bearer {API_KEY}"

request = Request(f"{API_URL}?limit=500", headers=headers)

with urlopen(request, timeout=30) as response:
    payload = json.load(response)

assert payload["dataset"] == "education.learning_outcomes"
assert payload["schema_version"] == "1.0"

print("API_FETCH_PASS")
print("records =", len(payload["data"]))
print("has_more =", payload["pagination"]["has_more"])

# NEXT in the existing project:
# 1) validate `payload` against the contract
# 2) use payload["data"] with the already implemented explicit Spark schema
# 3) add the 9 Bronze metadata fields
# 4) compute deterministic source-record SHA-256
# 5) write Parquet to MinIO Bronze
