# DAY 6 — TEACHING PROGRESS SECOND-DATASET REUSE PROOF

## 1. Executive Result

**Overall Status: PASS**

Day 6 onboarded a second dataset, **Teaching Progress**, through the reusable Lakehouse path and demonstrated that the generic transport, Bronze, and Silver mechanics can support a dataset with different schema, business key, DQ policy, and delete semantics without changing the frozen Learning Outcomes behavior.

- Branch: `feature/new-direction`
- Starting commit: `f507170ff69b9491acb40e9bfb4a34cfcce2d1e0`
- Day 6 code commit: `7c976a260fb0d0c713736e1de33d5455d422795f`
- Code commit message: `feat: onboard teaching progress through reusable lakehouse pipeline`
- Teaching source status: **MOCK / NOT VERIFIED CTU IOC PRODUCTION CONTRACT**
- Teaching Gold: **NOT IMPLEMENTED**
- Teaching Dashboard: **NOT IMPLEMENTED**
- Protected files changed: **0**
- Generic dataset-specific literal hits: **0**
- Duplicated Silver mechanics: **0**
- Blocking issues: **NONE**
- Recommendation: **READY FOR DAY 7**

This result proves reusable platform mechanics, not production correctness of the Teaching Progress business contract.

## 2. Source-Evidence Decision

Repository discovery found no verified CTU IOC Teaching Progress production contract, sample payload, production endpoint definition, field semantics, or existing dashboard schema.

Therefore Day 6 intentionally used a deterministic demo contract and explicitly did **not** present it as production truth.

Classification:

- contract type: `DEMO / ASSUMED CONTRACT`
- production verification: `NOT VERIFIED CTU IOC PRODUCTION CONTRACT`
- source system label: `ctu_ioc`
- dataset: `education.teaching_progress`
- schema version: `1.0-demo`
- demo endpoint: `/api/v1/education/teaching-progress`

Production compatibility claims are forbidden until real source evidence is available.

## 3. Teaching Grain and Business Key

### Demo grain

One Teaching Progress record represents:

> one course section in one academic year and semester, with a source-provided aggregate teaching-progress percentage.

This grain is an explicit Day 6 demo assumption, not verified production semantics.

### Business key

Canonical business key:

```text
(ma_lop_hoc_phan, nam_hoc, hoc_ky)
```

This key was established independently from Learning Outcomes and was not copied from:

```text
(ma_chuong_trinh, nam_hoc, hoc_ky)
```

### Record identity

Source record ID:

```text
<course_section_code>|<academic_year>|S<semester>
```

Baseline demo data contains 30 source records and 30 distinct Teaching business keys.

## 4. Source Contract

Source-native Teaching fields:

```text
record_id
unit_code
unit_name
course_section_code
academic_year
semester
progress_percent
updated_at
```

No synthetic numerator, denominator, attendance, grading, or session-level facts were invented.

The demo source contains 30 deterministic records generated without unseeded randomness.

## 5. Canonical Silver Contract

Source-to-canonical mapping:

| Source field | Canonical field |
|---|---|
| `record_id` | `ma_ban_ghi` |
| `unit_code` | `ma_don_vi` |
| `unit_name` | `ten_don_vi` |
| `course_section_code` | `ma_lop_hoc_phan` |
| `academic_year` | `nam_hoc` |
| `semester` | `hoc_ky` |
| `progress_percent` | `ty_le_tien_do_giang_day` |
| `updated_at` | `thoi_gian_cap_nhat_nguon` |

Targets:

```text
lakehouse.silver.teaching_progress
lakehouse.silver.teaching_progress_quarantine
```

The nine shared technical metadata fields remain:

```text
_source_system
_source_type
_dataset
_schema_version
_ingestion_mode
_batch_id
_source_updated_at
_ingested_at
_record_checksum
```

## 6. Delete Semantics

```text
DELETE_SUPPORTED = NO
```

No `is_deleted` or `da_xoa` field was invented for Teaching Progress.

The reusable classifier and MERGE mechanics were extended minimally to allow:

```text
delete_field=None
```

Learning Outcomes continues to use its existing configured delete field and frozen soft-delete behavior.

## 7. Teaching DQ Policy

Teaching owns seven Day 6 demo DQ rules:

1. `MISSING_RECORD_ID`
2. `MISSING_UNIT_CODE`
3. `MISSING_COURSE_SECTION_CODE`
4. `MISSING_ACADEMIC_YEAR`
5. `INVALID_SEMESTER`
6. `INVALID_TEACHING_PROGRESS_PERCENT`
7. `MISSING_SOURCE_UPDATED_AT`

The Learning Outcomes DQ rules were not reused as Teaching business policy.

`INVALID_TEACHING_PROGRESS_PERCENT` rejects values outside `0..100` at Silver DQ. The structural source contract deliberately does not convert every business-DQ rule into JSON Schema policy.

## 8. Reuse Result

| Capability | Day 6 result | Ownership |
|---|---|---|
| HTTP transport | REUSED | `HttpAdapter` |
| pagination | REUSED | generic HTTP adapter |
| API ingestion | REUSED | generic API ingestion |
| Bronze writer | REUSED | shared Bronze writer |
| nine technical metadata fields | REUSED | generic ingestion |
| checksum generation | REUSED | generic ingestion |
| source-native Bronze | REUSED invariant | generic ingestion / Bronze |
| source→canonical mapping timing | REUSED boundary | dataset wrapper after Bronze |
| DQ execution | REUSED | `generic_silver_quality.py` |
| quarantine mechanics | REUSED | `generic_silver_quarantine.py` |
| deterministic dedup | REUSED | `generic_silver_dedup.py` |
| equal-timestamp source conflict | REUSED | `generic_silver_conflict.py` |
| target-state classifier | REUSED / minimally extended | `generic_silver_classifier.py` |
| Iceberg MERGE mechanics | REUSED / minimally extended | `generic_silver_merge.py` |
| Nessie helper functions | REUSED | protected `nessie_catalog_utils.py` |
| dataset-specific lifecycle wrapper | RETAINED | Teaching wrapper |
| Teaching Gold | NOT IMPLEMENTED | out of scope |
| Teaching Superset dashboard | NOT IMPLEMENTED | out of scope |

Nessie lifecycle remains intentionally **PARTIALLY EXTRACTED / DATASET WRAPPER RETAINED**. Day 6 demonstrated a second wrapper using the same protected branch helper functions; it did not introduce a larger orchestration framework or callback/plugin abstraction.

## 9. Bronze Runtime Verification

Teaching runtime path:

```text
Mock Teaching API
→ HttpAdapter
→ generic API ingestion
→ Bronze Writer
→ MinIO source-native Bronze
```

Verified:

- source rows: 30
- Bronze rows: 30
- source-native business fields: 8
- technical metadata fields: 9/9
- canonical fields present before mapping: 0
- checksum stability errors across independent batch IDs: 0
- source-evolution extra field preserved in Bronze: YES
- evolved rows carrying the extra field: 30/30
- checksum changed when source content changed: 30/30
- evolved extra field automatically promoted to canonical Silver: NO

This preserves the architectural rule that canonical mapping occurs after Bronze.

## 10. Silver Runtime Verification

### Baseline / main publication

Teaching successfully traversed:

```text
Bronze
→ source→canonical mapping
→ Teaching DQ
→ generic source-conflict handling
→ generic deterministic dedup
→ generic target classifier
→ generic quarantine
→ generic MERGE
→ Iceberg/Nessie
```

Main Teaching state after scenario validation:

- rows: 30
- distinct business keys: 30
- duplicate business-key groups: 0
- quarantine rows on `main`: 0
- invalid progress rows on `main`: 0
- datasets present: only `education.teaching_progress`
- synthetic delete columns: none

### Idempotency

Identical rerun:

- Bronze: 30
- dedup: 30
- mergeable: 0
- duplicate: 30
- merge input: 0
- branch Silver: 30
- quarantine: 0

### Newer update

One deterministic newer observation:

- mergeable: 1
- duplicate: 29
- stale: 0
- merge input: 1
- branch Silver: 30
- main Silver after merge: 30

The update was read back from main successfully.

### Stale observation

- stale: 1
- duplicate: 29
- mergeable: 0
- merge input: 0

### Equal-timestamp source conflict

The corrected fixture used two distinct `record_id` values with the same Teaching business key and source timestamp but different content/checksum.

Result:

- Bronze: 31
- DQ invalid: 0
- source conflicts: 2
- dedup: 29
- mergeable: 0
- duplicate: 29
- quarantined: 2
- merge input: 0
- branch Silver: 30
- duplicate business keys: 0
- both rejection reasons: `EQUAL_TIMESTAMP_DIFFERENT_CHECKSUM`

This confirms the frozen whole-business-key conflict policy.

### Business DQ quarantine

A branch-only record with `progress_percent=120` produced:

- DQ invalid: 1
- dedup: 29
- duplicate: 29
- quarantined: 1
- merge input: 0
- branch Silver: 30
- reason: `INVALID_TEACHING_PROGRESS_PERCENT`

Branch-only conflict and DQ scenarios did not modify Teaching main.

## 11. Runtime-State Note

During 6H, one deterministic Teaching newer-update scenario was intentionally published to `main`. A later state-aware recovery published one further deterministic newer observation for the same demo record.

Therefore the final Teaching main table is **not a pristine copy of the original 30-row JSON baseline**. It is the expected validated scenario state:

- still 30 rows
- still 30 business keys
- quarantine 0
- no invalid rows
- state checksum audit passed
- later branch-only conflict/DQ scenarios left main unchanged

This is expected demo-test state, not an unexplained mutation.

## 12. Learning Outcomes Regression

Learning Outcomes remained frozen throughout Day 6.

Final main verification:

```text
Silver rows                 = 30
Silver active               = 30
Silver deleted              = 0
Silver business keys        = 30
Silver duplicate key groups = 0
Silver quarantine           = 0
Gold rows                   = 30
Gold keys                   = 30
```

Gold verification:

```text
pass-rate mismatch             = 0
GPA mismatch                   = 0
on-track mismatch              = 0
lineage mismatch               = 0
first-period trend invalid     = 0
later-period previous missing  = 0
pass-rate trend mismatch       = 0
GPA trend mismatch             = 0
on-track trend mismatch        = 0
```

Learning dataset identity remained only:

```text
education.learning_outcomes
```

The Learning Silver wrapper, Learning Gold code, and `HttpAdapter` had no Day 6 diff.

## 13. Multi-Dataset Coexistence

Final main coexistence:

| Check | Learning Outcomes | Teaching Progress |
|---|---:|---:|
| Silver rows | 30 | 30 |
| distinct business keys | 30 | 30 |
| duplicate key groups | 0 | 0 |
| main quarantine rows | 0 | 0 |
| dataset identity mismatches | 0 | 0 |
| expected delete support | YES | NO |

No Teaching path/table/config/business-key/schema leakage into Learning was observed.

No Learning path/table/config/business-key/DQ policy was reused blindly for Teaching.

## 14. Trino Verification

Read-only Trino results:

```text
Learning Silver: 30 rows / 30 active / 0 deleted / 30 keys
Teaching Silver: 30 rows / 30 keys / 0 dataset mismatches / 0 invalid progress / 0 duplicate groups
Quarantine:      Learning 0 / Teaching 0
Learning Gold:   30 rows / 30 keys
```

All Trino query exit codes were `0`.

The terminal emitted the known non-blocking JLine dumb-terminal warning; query results and exit codes remained successful.

## 15. Superset Verification

Existing Learning Outcomes Superset objects remained unchanged:

- datasets: 1
- charts: 2
- dashboards: 1
- required filters preserved:
  - `nam_hoc`
  - `hoc_ky`
  - `ten_chuong_trinh`

Teaching Superset datasets:

```text
0
```

No Teaching dashboard was built.

## 16. Generic Boundary Audit

Final generic boundary checks:

```text
GENERIC_DATASET_LITERAL_HIT_COUNT = 0
DUPLICATED_MECHANICS_TOKEN_HIT_COUNT = 0
DUPLICATED_MECHANICS = 0
```

The generic Silver modules do not contain Teaching-specific field names, table names, dataset IDs, or Learning-vs-Teaching dataset branches.

Teaching does not copy generic MERGE SQL, windowed dedup implementation, conflict implementation, or quarantine implementation.

## 17. Automated Tests

Final Day 6 regression status:

- Python compile: PASS
- HTTP + source contract suite: PASS
- Learning registry tests: 9/9 PASS
- Teaching registry tests: 8/8 PASS
- Generic Silver tests: 42/42 PASS
- Learning Outcomes behavior freeze: 18/18 PASS
- Teaching Silver foundation + orchestration: 13/13 PASS
- generic boundary/literal guard: PASS
- two-dataset runtime regression: PASS
- Learning Gold regression: PASS
- Trino two-dataset verification: PASS
- Superset read-only regression: PASS

Earlier combined HTTP/source-contract execution recorded 40 passing tests after Teaching contract onboarding; the final 6I rerun also exited successfully.

## 18. Test-Harness Incidents

Two Day 6 incidents were test-harness/fixture issues and did not require production-source changes.

### PowerShell native stderr handling

Spark emits:

```text
load-spark-env.sh: line 68: ps: command not found
```

Windows PowerShell with `$ErrorActionPreference="Stop"` initially treated native stderr as a terminating error even though this warning had been non-blocking in prior checkpoints.

Recovery changed only the test harness to use the native process exit code as the authoritative result.

### Initial conflict fixture violated record-ID uniqueness

The first conflict fixture duplicated the same source `record_id`, causing generic Bronze read-back validation to reject the fixture before Silver conflict processing.

The fixture was corrected to:

- distinct source `record_id`
- same Teaching business key
- same source timestamp
- different content/checksum

No source implementation change was required.

## 19. Protected / Out-of-Scope Safety

Protected files changed:

```text
0
```

Protected files:

```text
lakehouse/dags/lakehouse_pipeline.py
lakehouse/spark/spark_ingest_bronze.py
lakehouse/spark/spark_bronze_to_silver.py
lakehouse/spark/spark_silver_to_gold.py
lakehouse/spark/nessie_catalog_utils.py
```

Additional unchanged existing components:

```text
lakehouse/spark/spark_learning_outcomes_to_silver.py
lakehouse/spark/spark_learning_outcomes_to_gold.py
lakehouse/spark/http_adapter.py
```

Teaching Gold: **NOT IMPLEMENTED**

Teaching Dashboard: **NOT IMPLEMENTED**

Legacy DAG integration: **NOT IMPLEMENTED**

## 20. Day 6 Code Scope

Day 6 code commit staged exactly 13 files and no protected file.

Modified:

```text
ctu_ioc_mock_api/app/main.py
lakehouse/spark/api_dataset_registry.py
lakehouse/spark/generic_silver_classifier.py
lakehouse/spark/generic_silver_merge.py
lakehouse/spark/tests/test_generic_silver_classifier.py
lakehouse/spark/tests/test_generic_silver_merge.py
```

Added:

```text
ctu_ioc_mock_api/app/data/teaching_progress.json
lakehouse/spark/contracts/teaching_progress.schema.json
lakehouse/spark/spark_teaching_progress_to_silver.py
lakehouse/spark/tests/test_teaching_progress_registry.py
lakehouse/spark/tests/test_teaching_progress_silver_foundation.py
lakehouse/spark/tests/test_teaching_progress_silver_orchestration.py
tests/test_teaching_progress_contract.py
```

Code checkpoint:

```text
7c976a260fb0d0c713736e1de33d5455d422795f
```

## 21. Acceptance Checklist

- [x] Day 5 baseline re-verified before Teaching implementation
- [x] Production Teaching source evidence searched before contract design
- [x] Unknown production semantics explicitly marked as unknown
- [x] Demo contract explicitly marked as assumed
- [x] Teaching grain established before schema implementation
- [x] Teaching business key established independently
- [x] Deterministic 30-row mock source
- [x] Same reusable `HttpAdapter`
- [x] Same generic API ingestion
- [x] Same Bronze Writer
- [x] Source-native Bronze
- [x] Same nine technical metadata fields
- [x] Checksum stable across batch IDs
- [x] Source content change changes checksum
- [x] Source-evolution extra field preserved in Bronze
- [x] Extra field not automatically promoted to canonical Silver
- [x] Same dataset registry
- [x] Teaching canonical schema distinct from Learning schema
- [x] Teaching DQ rules dataset-specific
- [x] Delete semantics not invented
- [x] Generic no-delete capability verified
- [x] Generic DQ mechanics reused
- [x] Generic quarantine mechanics reused
- [x] Generic dedup mechanics reused
- [x] Generic conflict mechanics reused
- [x] Generic classifier reused
- [x] Generic MERGE reused
- [x] Nessie protected helpers reused
- [x] Branch isolation verified
- [x] main publication verified
- [x] rerun/idempotency verified
- [x] newer update verified
- [x] stale classification verified
- [x] equal-timestamp conflict verified
- [x] DQ quarantine verified
- [x] two-dataset coexistence verified
- [x] generic dataset-specific literal hits = 0
- [x] duplicated mechanics = 0
- [x] Learning behavior freeze remains green
- [x] Learning Gold formulas/trends/lineage remain green
- [x] Trino both datasets PASS
- [x] Superset Learning regression PASS
- [x] protected files changed = 0
- [x] Teaching Gold NOT implemented
- [x] Teaching Dashboard NOT implemented
- [x] exact Day 6 code scope staged
- [x] code commit created

## 22. Remaining Limitations

1. Teaching Progress source contract is not production-verified.
2. Teaching grain and DQ policy are demo assumptions pending business/source confirmation.
3. Teaching has no verified delete semantics; current `DELETE_SUPPORTED=NO` is the demo-contract position.
4. Teaching Gold and dashboard are intentionally out of scope.
5. Nessie lifecycle remains wrapper-owned rather than fully genericized.
6. Day 6 leaves audit branches for inspection according to the existing lifecycle behavior.
7. Final Teaching main contains an intentional newer-update demo state rather than the pristine baseline payload.

None of these items blocks the Day 6 reuse objective.

## 23. Behavior Changes

Expected behavior changes:

```text
Teaching Progress dataset added.
Generic classifier/MERGE can now support a configured dataset without a delete field.
```

The optional no-delete capability does not change Learning Outcomes behavior.

Unexpected behavior changes:

```text
NONE observed.
```

## 24. Final Day 6 Result

```text
DAY 6 RESULT
============

Overall Status:
PASS

Starting Commit:
f507170ff69b9491acb40e9bfb4a34cfcce2d1e0

Ending Commit:
7c976a260fb0d0c713736e1de33d5455d422795f

Teaching Source Status:
MOCK / NOT VERIFIED CTU IOC PRODUCTION CONTRACT

Teaching Grain:
one course section × one academic year × one semester,
with source-provided aggregate teaching-progress percentage

Teaching Business Key:
(ma_lop_hoc_phan, nam_hoc, hoc_ky)

Source Contract:
education.teaching_progress / 1.0-demo / 8 source-native fields

Canonical Silver Contract:
8 canonical business fields + 9 technical metadata fields
Silver: lakehouse.silver.teaching_progress
Quarantine: lakehouse.silver.teaching_progress_quarantine

Rows:
- source: 30
- Bronze baseline: 30
- valid baseline: 30
- invalid baseline: 0
- main quarantine: 0
- main Silver: 30
- main Silver business keys: 30

Reuse:
- HttpAdapter: REUSED
- ingestion: REUSED
- Bronze Writer: REUSED
- metadata: REUSED
- checksum: REUSED
- mapping boundary: REUSED; dataset mapping remains specific
- DQ engine: REUSED
- quarantine: REUSED
- dedup: REUSED
- conflict: REUSED
- classifier: REUSED; optional delete-field support added generically
- merge: REUSED; optional delete-field support added generically
- Nessie: PARTIALLY EXTRACTED / DATASET WRAPPER RETAINED

Duplicated Mechanics:
0

Learning Regression:
PASS — Silver 30/30 active/0 deleted/30 keys/quarantine 0;
Gold 30/30; all formula/trend/lineage mismatches 0

Multi-Dataset Coexistence:
PASS

Generic Engine Dataset-Specific Literals:
0

Automated Tests:
PASS

Protected Files Changed:
0

Teaching Gold:
NOT IMPLEMENTED

Teaching Dashboard:
NOT IMPLEMENTED

Expected Behavior Changes:
Teaching Progress dataset added; generic no-delete configuration supported.

Unexpected Behavior Changes:
NONE observed.

Blocking Issues:
NONE

Recommendation:
READY FOR DAY 7
```

## 25. Rollback Chain

```text
DAY 1
1f2d03965a2847855583a539982ca6e8f7e6e8e5
        ↓
DAY 2
f455d955bf88b15686e3e12e9a341e65e89316a8
        ↓
DAY 3
1c3c13db68542b6798feeee4ad192f3f18876d43
        ↓
DAY 4
73bb01cd160a0d3dc23e91e9c6a5991cd564f7c1
        ↓
DAY 5 CODE
68ff1e55f1690103e11bbe931d1da31150e124b1
        ↓
DAY 5 DOCS
f507170ff69b9491acb40e9bfb4a34cfcce2d1e0
        ↓
DAY 6 CODE
7c976a260fb0d0c713736e1de33d5455d422795f
```

---

# DAY 6 CONCLUSION

Day 6 answered the architectural question:

> Can the platform onboard a second dataset without copying the first dataset's pipeline or contaminating the reusable engine with dataset-specific business logic?

For the verified demo scope, the answer is **yes**.

The result must not be interpreted as verification of a real CTU IOC Teaching Progress production API or business contract.
