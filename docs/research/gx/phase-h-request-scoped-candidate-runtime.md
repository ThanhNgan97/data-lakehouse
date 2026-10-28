# Phase H - Request-scoped candidate validation runtime

Phase H makes the GX service execute registered SQL suites against a Nessie
candidate branch. Candidate requests no longer have to match the Trino
catalog's static `main` reference.

Trino remains the engine for the catalog branch configured by
`GX_TRINO_NESSIE_BRANCH`. A request for any other branch is routed to a lazy
Spark/Iceberg runtime when `GX_CANDIDATE_SPARK_ENABLED=true`. Disabling that
runtime is fail-closed; candidate requests receive a configuration error rather
than falling back to `main`.

Each branch receives a deterministic, isolated Spark catalog alias configured
with its own Nessie `ref`. The existing snapshot-aware gate still resolves and
compares branch, Iceberg snapshot, and commit before suite execution and again
before issuing approval.

The candidate executor consumes the same reviewed SQL suite registry as Trino.
It handles schema expectations from the loaded Iceberg DataFrame and executes
the remaining scalar expectations with Spark SQL. The bounded Trino
`date_diff('hour', ...)` expression is translated to Spark's
`timestampdiff(HOUR, ...)`; asset references are rewritten only to the internal
catalog alias, never to a caller-selected table.

Spark starts lazily on the first candidate request, reuses one Spark context,
and creates isolated SQL sessions/catalogs for branch requests. Shutdown closes
both the Trino pool and Spark runtime.

## Cutover status

The service is now capable of validating the candidate request produced in
phase G. The API DAG is intentionally unchanged in this phase. The next phase
must replace the writer/gate tasks together and introduce guarded publication;
switching only the writer would leave valid data unmerged.
