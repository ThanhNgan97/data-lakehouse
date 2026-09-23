# CTU IOC Real API Onboarding Checklist

Use this checklist only after CTU IOC provides an approved API contract and non-production access where possible.

## Phase 1 — Contract intake

- [ ] 1. Receive authoritative external API contract and named business/technical owners.
- [ ] 2. Confirm environment URLs and network access requirements.
- [ ] 3. Confirm authentication method, token lifecycle and required headers.
- [ ] 4. Confirm pagination, filtering, rate limits, error model and SLA.
- [ ] 5. Capture representative payload samples from an approved environment.
- [ ] 6. Complete `CTU_IOC_SOURCE_CONTRACT_TEMPLATE.csv`.
- [ ] 7. Classify PII/sensitive fields and obtain security handling requirements.

## Phase 2 — Contract comparison

- [ ] 8. Compare real source schema against current mock assumptions.
- [ ] 9. Confirm schema/API version semantics.
- [ ] 10. Confirm stable record identifier.
- [ ] 11. Confirm business grain and business-key candidates with the business owner.
- [ ] 12. Confirm created/updated timestamps and timezone.
- [ ] 13. Confirm append/update/delete/late-arrival semantics.
- [ ] 14. Confirm official DQ rules and quarantine policy.
- [ ] 15. Classify every difference as CONFIG ONLY, DATASET CONTRACT CHANGE, DATASET BUSINESS LOGIC REVIEW or PLATFORM CODE CHANGE.

## Phase 3 — Minimal implementation

- [ ] 16. Configure real base URL/resource path without hard-coding secrets.
- [ ] 17. Configure authentication through approved secret management.
- [ ] 18. Update Dataset Registry contract/config as required.
- [ ] 19. Update source → canonical mapping as required.
- [ ] 20. Do **not** rewrite Generic Silver unless an actual unsupported platform requirement is proven.
- [ ] 21. Do **not** change Gold formulas until business semantics are confirmed.

## Phase 4 — Controlled validation

- [ ] 22. Run endpoint/auth connectivity validation separately.
- [ ] 23. Run contract/schema tests.
- [ ] 24. Run API → Bronze only.
- [ ] 25. Verify Bronze preserves source-native fields and technical metadata/checksum.
- [ ] 26. Reconcile source count/identity with Bronze.
- [ ] 27. Run Bronze → Silver on an isolated/controlled Nessie branch where appropriate.
- [ ] 28. Verify DQ, quarantine, duplicate/conflict and stale-update behavior.
- [ ] 29. Verify Silver grain/business keys.
- [ ] 30. Run Gold only after Silver semantics are accepted.
- [ ] 31. Recalculate/reconcile all business metrics independently.
- [ ] 32. Verify lineage from Gold back to Silver/Bronze.
- [ ] 33. Validate Trino queries and row/grain reconciliation.
- [ ] 34. Validate Superset datasets, filters, labels and business meaning.
- [ ] 35. Run `api_dataset_pipeline` end-to-end through Airflow UI.
- [ ] 36. Confirm all expected tasks are visible and failures are not swallowed.

## Phase 5 — Production-readiness gate

- [ ] 37. Validate retry/backoff behavior against provider SLA, especially HTTP 429/5xx/timeouts.
- [ ] 38. Externalize and rotate secrets; verify no production secret is committed.
- [ ] 39. Define operational alerting, log retention, ownership and escalation.
- [ ] 40. Define schema-change/deprecation process.
- [ ] 41. Complete security review and access-control review.
- [ ] 42. Obtain business-owner signoff for grain, keys, DQ and Gold semantics.
- [ ] 43. Obtain technical-owner signoff for API/operations behavior.
- [ ] 44. Only then update documentation from `NOT VERIFIED PRODUCTION` to the evidence-supported production status.

## Stop conditions

Stop onboarding and do not promote downstream trust if any of the following remains unresolved:

- Authentication cannot be validated.
- Business grain/key is ambiguous.
- Required fields or update/delete semantics are unknown.
- Pagination can omit/duplicate data without a documented handling rule.
- Production DQ policy is not approved.
- Gold metric meaning cannot be reconciled.
- Secrets would need to be committed into the repository.
- Breaking schema differences exist without an approved mapping/contract update.
