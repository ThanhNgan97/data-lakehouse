# Phase 12 - GX-guarded API Silver publication

Phase 12 cuts the registered API Silver flow over to the candidate workflow:

```text
prepare-silver-candidate
  -> gx_validate_silver
  -> publish_silver_candidate
  -> validate-silver on main
```

The publisher derives candidate state from dataset/run identity and accepts only
the bounded GX approval XCom. Dataset, asset, branch, snapshot, candidate commit,
and validation ID must all be present and match. Immediately before merge it
resolves every affected candidate asset again, including quarantine state.

Publication uses Nessie API v2 with both optimistic locks:

- the target reference is `main@base_hash`;
- the merge payload fixes `fromRefName` and `fromHash` to the approved candidate.

If `main` moved after candidate creation, publication fails and requires a new
candidate. If the merge succeeded but the Airflow response was lost, a retry is
idempotent: the publisher verifies that all recorded snapshots are already on
`main` and returns `ALREADY_PUBLISHED` without another merge. Every successful
merge is followed by the same multi-asset verification.

The portal now displays Silver candidate creation, GX, and Silver publication as
separate stages. Gold publication remains followed by GX Gold in the current
DAG; moving Gold and reconciliation before publication is a later cutover.
