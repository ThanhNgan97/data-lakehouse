import json
import os
import sys
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8090"
url = f"{BASE_URL}/api/v1/education/learning-outcomes?limit=5"
login_request = urllib.request.Request(
    f"{BASE_URL}/api/v1/auth/login",
    data=json.dumps({
        "username": os.getenv("MOCK_API_USERNAME", "lakehouse_test"),
        "password": os.getenv("MOCK_API_PASSWORD", "Lakehouse@123"),
    }).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)

with urllib.request.urlopen(login_request, timeout=10) as response:
    access_token = json.load(response)["access_token"]

data_request = urllib.request.Request(
    url,
    headers={"Authorization": f"Bearer {access_token}"},
)
with urllib.request.urlopen(data_request, timeout=10) as response:
    payload = json.load(response)

assert payload["dataset"] == "education.learning_outcomes"
assert payload["schema_version"] == "2.0"
assert payload["pagination"]["returned_records"] == 5
assert len(payload["data"]) == 5

print("MOCK_API_CONNECTION_PASS")
print("dataset =", payload["dataset"])
print("schema_version =", payload["schema_version"])
print("returned_records =", payload["pagination"]["returned_records"])
print("has_more =", payload["pagination"]["has_more"])
print("next_cursor =", payload["pagination"]["next_cursor"])
first = payload["data"][0]
expected_fields = {
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
assert expected_fields.issubset(first)
assert "ma_ban_ghi" not in first

print("first_record_id =", first["record_id"])
