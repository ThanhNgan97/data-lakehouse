# CTU IOC API Handoff Requirements

## Purpose

This document defines the information required from CTU IOC before the current Data Lakehouse can be connected to a real external API contract.

The current platform has been validated end-to-end with deterministic mock API contracts. This document does **not** assume that current mock field names, demo business rules, endpoint structure, authentication, pagination, or update semantics are the CTU IOC production contract.

## A. API ownership

Please provide:

- System/API name.
- Business owner.
- Technical owner/contact.
- Environment names (development, test/UAT, production).
- Escalation/contact path for incidents and contract changes.

## B. Endpoint information

For each environment and resource:

- Base URL.
- Resource route.
- API version.
- HTTPS/TLS requirements.
- Health/status endpoint, if available.
- Any gateway or network restrictions (VPN, allow-list, private network, proxy).

Do not send production secrets in this document.

## C. Authentication and authorization

Please specify:

- Authentication method (API key, OAuth2, bearer token, mTLS, signed request, other).
- Token acquisition flow.
- Token lifetime/expiry.
- Refresh/renewal mechanism.
- Required headers.
- Scope/role requirements.
- Environment-specific differences.
- Secret-delivery process approved by CTU IOC.

Production secret values must not be committed to source control.

## D. Dataset identity and business grain

For each dataset/resource provide:

- Dataset/resource name.
- Business description.
- Record grain: what exactly one API record represents.
- Business owner.
- Whether the resource is snapshot, event, aggregate, master data or transaction data.
- Candidate natural/business key.

The current demo keys must not be assumed to be production keys.

## E. Field contract

For every source field provide:

- Source field name.
- Business description.
- Data type.
- Nullable?
- Required?
- Example value.
- Business meaning/units.
- Enumerated values or valid range, if applicable.
- PII/sensitive-data classification.

Use `CTU_IOC_SOURCE_CONTRACT_TEMPLATE.csv` for this handoff.

## F. Identifiers

Confirm:

- Stable source record ID, if any.
- Candidate business key(s).
- Whether IDs are globally unique or scoped by another field.
- Whether IDs can be reused.
- Whether keys can change after creation.

## G. Time semantics

Confirm:

- Created timestamp.
- Last-updated timestamp.
- Source timezone.
- Timestamp precision and format.
- Academic-year and semester/term semantics.
- Whether timestamps represent business time, ingestion time or last modification time.

## H. Update and delete semantics

For each resource specify:

- Append-only, mutable or mixed.
- Full snapshot or incremental feed.
- Incremental cursor/filter, if supported.
- Soft-delete field and meaning.
- Hard-delete behavior.
- Late-arriving records.
- Backfills/corrections.
- Whether an older update can legitimately arrive after a newer one.

## I. Pagination

Specify exactly:

- Cursor, offset, page number or token style.
- Page-size parameter and maximum.
- Response field that contains the next-page token.
- End-of-pagination behavior.
- Stability guarantees while pages are being read.
- Whether pages can repeat or omit records under concurrent updates.

Current platform code has validated cursor-style mock pagination only; other styles may require a controlled adapter extension.

## J. Filtering and incremental extraction

Confirm support for:

- `updated_after` / `updated_since` equivalent.
- Date/time windows.
- Dataset/resource filters.
- Server-side sorting guarantees.
- Whether inclusive/exclusive timestamp boundaries are used.

## K. Rate limits and retry policy

Provide:

- Requests per second/minute/day.
- Burst allowance.
- HTTP 429 behavior.
- `Retry-After` semantics, if used.
- Retryable vs non-retryable status codes.
- Recommended backoff.
- Maintenance-window behavior.

The current platform has no specialized production 429/backoff policy; it should not be invented without provider guidance.

## L. Error model

Provide:

- Possible HTTP status codes.
- Error response schema.
- Error code catalogue.
- Retryable vs non-retryable errors.
- Partial-success behavior, if any.
- Correlation/request ID fields.
- Support contact path.

## M. Data-quality and business semantics

Please confirm:

- Required fields.
- Valid ranges.
- Enumerations.
- Cross-field constraints.
- Duplicate handling expectations.
- Official business rules for quality checks.
- Rules that should quarantine a record vs fail the batch.

Current demo DQ rules must not be treated as official CTU IOC policy without business-owner signoff.

## N. SLA / operational expectations

Provide:

- Availability target.
- Refresh/update frequency.
- Expected data latency.
- Maintenance windows.
- Maximum expected payload/page size.
- Recovery expectations after outage.

## O. Schema evolution and change management

Please define how CTU IOC will communicate:

- New fields.
- Removed fields.
- Optional-to-required changes.
- Type changes.
- Enumeration changes.
- New API/schema versions.
- Breaking changes.
- Deprecation windows.
- Effective dates and test/UAT availability.

## Acceptance condition before production integration is declared

Production integration can be declared only after:

1. The authoritative external contract is received.
2. Endpoint/authentication is validated without committing secrets.
3. Representative payloads are captured from an approved environment.
4. Source schema, business keys, update/delete semantics and DQ rules are confirmed.
5. Contract tests pass.
6. API → Bronze validation passes with source-native preservation.
7. Silver/Gold semantics are reconciled.
8. Trino and Superset serving are validated.
9. The common Airflow DAG completes successfully.
10. Business and technical owners approve the result.

Until then, the safe statement is:

> The platform is architecturally ready for controlled onboarding of a real CTU IOC API contract; production API compatibility has not yet been verified.
