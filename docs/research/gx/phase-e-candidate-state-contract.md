# Phase E - Durable candidate-state contract

Phase E introduces the durable hand-off between a lakehouse writer, GX, and a
future publisher. It records the exact state produced on a Nessie candidate
branch instead of asking a later task to rediscover mutable state.

The versioned document contains:

- dataset, source, and Airflow run identity;
- retry-stable candidate branch and its base reference;
- base hash and candidate head hash;
- every affected canonical Silver or Gold asset and its Iceberg snapshot ID.

Candidate state is validated on both construction and load. It cannot target
`main`, contain an unsafe table identifier, omit assets, or contain the same
asset twice. State files are written atomically and their paths are derived from
a hash of dataset/run identity, so caller input cannot escape the configured
state root. A retry may replay an identical document, but cannot replace an
existing run with a different branch, commit, or snapshot identity.

Branch names are deterministic for one dataset/run. Airflow retries therefore
reuse the same candidate identity instead of silently creating another branch;
different runs receive different names.

## Cutover boundary

This phase deliberately does not disable the existing processor-owned merges.
Doing so before the DAG has a publication task would strand valid runs. The
next cutover must make writers persist this state without merging, make GX read
the recorded candidate identity, and add a publisher that accepts only matching
GX approval evidence with an expected-hash/CAS merge.
