# Phase F - Registered API Silver candidate writer

Phase F adds a non-publishing writer path for registered API datasets without
changing the production DAG dependency chain yet.

The new `prepare-silver-candidate` command:

1. requires the registered Silver and quarantine tables to have been
   bootstrapped already, preventing initialization from mutating `main`;
2. captures the current `main` hash;
3. selects a retry-stable candidate branch from dataset/run identity;
4. calls the configured dataset processor with `merge_to_main=False`;
5. resolves both affected Iceberg tables at one immutable candidate head;
6. atomically persists the phase-E candidate-state document.

Teaching Progress and Learning Outcomes processors now accept an injected
candidate branch. The existing `process-silver` command retains its behavior so
the currently deployed DAG remains functional until GX can read the candidate
branch and a guarded publisher exists.

Snapshot capture includes Nessie's `hashOnRef` parameter. The table pointer and
candidate commit therefore cannot be combined from two different moments if a
concurrent writer advances the branch.

## Next cutover

Phase G must make the GX Silver gate load this candidate state and query its
branch. Only after the guarded publish phase is available should the API DAG
replace `process-silver` with `prepare-silver-candidate`.
