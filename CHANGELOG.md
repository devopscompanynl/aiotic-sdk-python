# Changelog

All notable changes to the AIOTIC Python SDK (`aiotic-sdk`). The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).
Dates are release dates. The full documentation is at <https://developers.aiotic.ai>.

## Unreleased

## 0.1.0 — 2026-10-04

First release. Verified against AIOTIC API 1.0.0.

- `AioticClient` and `AsyncAioticClient`: a typed method for every public endpoint, retries with backoff,
  client-side rate limiting and automatic routing of master-data calls to the sync key.
- `aiotic.models`: Pydantic models pinned to the public OpenAPI document, including the order status values.
- `aiotic.receive`: the ERP receive endpoint with API-key check, idempotency and the `{success, …}` response contract.
- `aiotic.pipeline`: sanitizers, validators and business rules for ERPs that expose only a data API.
- `aiotic.erp`: the `ErpPort` interface with `FunctionalApiAdapter`, `DataApiAdapter` and `InMemoryErp` templates.
- `aiotic.sync`: event-driven master-data sync with hash-based reconciliation, never a full re-upload.
- `aiotic.watch`: `OrderWatcher`, which polls order status and emits transitions.
- `aiotic.webhooks` and `aiotic.service`: receivers for the processing webhook and your ERP's change events, and
  `build_app()` for a complete integration service in one call.
- The `aiotic` command line (`init`, `doctor`, `serve`, `mock`) and `aiotic.mock`, a mock AIOTIC tenant for local
  development and CI.
