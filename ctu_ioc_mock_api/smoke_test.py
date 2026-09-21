import json
import sys
import urllib.request

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8090"
url = f"{BASE_URL}/api/v1/education/learning-outcomes?limit=5"

with urllib.request.urlopen(url, timeout=10) as response:
    payload = json.load(response)

assert payload["dataset"] == "education.learning_outcomes"
assert payload["schema_version"] == "1.0"
assert payload["pagination"]["returned_records"] == 5
assert len(payload["data"]) == 5

print("MOCK_API_CONNECTION_PASS")
print("dataset =", payload["dataset"])
print("schema_version =", payload["schema_version"])
print("returned_records =", payload["pagination"]["returned_records"])
print("has_more =", payload["pagination"]["has_more"])
print("next_cursor =", payload["pagination"]["next_cursor"])
print("first_record_id =", payload["data"][0]["ma_ban_ghi"])
