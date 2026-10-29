# Phase G - Candidate-aware registered API GX gate

Phase G adds a separate GX gate entrypoint for the registered API Silver
candidate. It does not silently fall back to `main`.

The gate derives the phase-E state path from dataset/run identity, loads the
immutable candidate document, and requires matching dataset, run, and API
source. It selects the canonical Silver asset from the reviewed GX route rather
than trusting a table supplied by the caller.

Before calling GX, the gate resolves that asset again at the recorded candidate
branch. Snapshot ID and branch head must still equal the writer's state. A
changed branch is rejected as stale and no validation request is sent. The GX
request then carries the recorded candidate branch, snapshot, and commit, and
automatic snapshot refresh is disabled so a retry cannot authorize a different
candidate.

The existing `validate_api_dataset` main-branch entrypoint remains unchanged and
the production DAG still uses it. The new candidate entrypoint must not be wired
into the DAG until the GX runtime can execute SQL suites against a request-scoped
Nessie branch; the current Trino catalog is mounted to `main` and correctly
rejects any other branch.
