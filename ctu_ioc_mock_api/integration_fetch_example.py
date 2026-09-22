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
assert payload["schema_version"] == "2.0"

first = payload["data"][0]
expected_source_fields = {
    "record_id",
    "program_code",
    "program_name",
    "academic_year",
    "semester",
    "student_count",
    "passed_course_count",
    "attempted_course_count",
    "gpa_point_sum",
    "gpa_student_count",
    "warning_student_count",
    "dropout_risk_student_count",
    "on_track_student_count",
    "progress_evaluated_student_count",
    "updated_at",
    "is_deleted",
}
assert expected_source_fields.issubset(first)

print("API_FETCH_PASS")
print("schema_version =", payload["schema_version"])
print("records =", len(payload["data"]))
print("has_more =", payload["pagination"]["has_more"])
print("first_record_id =", first["record_id"])

# NEXT in the existing project:
# 1) validate `payload` against the contract
# 2) use payload["data"] with the configured source-native Spark schema
# 3) add the 9 Bronze metadata fields
# 4) compute deterministic source-record SHA-256
# 5) write source-native Parquet to MinIO Bronze
# 6) map configured source fields to the canonical Silver field names
