# GX remediation phase A: current safety baseline

Date: 2026-10-28

## Purpose

Phase A records the current orchestration boundaries before candidate-branch
cutover. These are characterization tests, not acceptance of the current
architecture. Later phases must deliberately replace the assertions when the
production flow changes.

## Confirmed gaps

1. Dynamic file uploads return `NOT_CONFIGURED` without invoking the GX
   service, so they receive no baseline validation.
2. Dynamic Airbyte/database contexts also return `NOT_CONFIGURED` without a
   baseline validation.
3. Registered file and database gates resolve the `main` branch by default;
   they are post-write checks rather than universal pre-merge gates.
4. API Silver processing precedes `gx_validate_silver`, and the processor still
   uses `merge_to_main=True`.
5. `publish_gold` precedes `gx_validate_gold`, so the separate GX Gold task
   cannot block the publication it is intended to protect.

## Safety invariants already covered

The snapshot-aware runtime tests require an exact match of asset, branch,
snapshot ID, and Nessie commit hash. They also reject non-executed validation,
quality-policy failure, and identity changes. These invariants must be retained
when candidate publication is connected to the DAGs.

## Required replacement behavior

- Every source receives baseline validation.
- Silver and Gold are written to retry-stable candidate state.
- GX validates the sealed candidate identity.
- Publication verifies the same identity immediately before a CAS commit.
- A failed, unavailable, stale, or revoked validation never changes `main`.

The decisive end-to-end assertion for each source is:

```text
main_hash_after == main_hash_before
```

whenever validation or approval fails.
