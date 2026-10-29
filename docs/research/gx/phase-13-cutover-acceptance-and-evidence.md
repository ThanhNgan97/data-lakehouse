# Phase 13 - Cutover acceptance and publication evidence

Phase 13 closes the registered API Silver cutover with operator-visible
evidence. The portal no longer treats a GX `ALLOWED` result as proof that data
was published. It reads the bounded XCom from `publish_silver_candidate` and
shows candidate branch, approved candidate commit, final publication commit,
and publication status.

The accepted Silver sequence is now:

1. write Silver and quarantine changes only on a retry-stable candidate;
2. persist immutable candidate state;
3. validate the recorded candidate with GX;
4. require an exact approval receipt;
5. merge with target-base and candidate-head optimistic locks;
6. verify every recorded asset on `main`;
7. validate the published Silver readback;
8. expose both validation and publication evidence in the API portal.

An interrupted retry is fail-closed. A complete state document is replayed
without rewriting the branch. A branch that exists without durable state is
treated as an unsafe partial attempt and is not reused automatically.

## Remaining boundary

This acceptance applies to registered API Silver. Gold is still built on a dbt
candidate and atomically published by the existing dbt contract, but GX Gold
and Silver-Gold reconciliation currently run after that publication. The portal
states this explicitly. The system must not claim that GX prevents invalid Gold
publication until those two checks are moved onto the dbt candidate bundle.
