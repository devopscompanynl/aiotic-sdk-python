# Changelog

All notable changes to the AIOTIC Python SDK (`aiotic-sdk`). The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow [Semantic Versioning](https://semver.org/).
Dates are release dates. The full documentation is at <https://developers.aiotic.ai>.

## Unreleased

## 0.1.2 — 2026-10-04

### Fixed

- The README's changelog and license links also work when viewed on PyPI.

## 0.1.1 — 2026-10-04

First release on PyPI: `pip install "aiotic-sdk[all]"`. Verified against AIOTIC API 1.0.0.

### Fixed

- `AioticClient` and `AsyncAioticClient` work on a private copy of the `Settings` they are given. Two clients that
  share one `Settings` object, or a caller that keeps changing it, no longer affect each other's base URL or keys.
- Identifiers in URLs (customer numbers, item numbers, language codes, customer item numbers, file names, search
  text) are percent-encoded as one path segment, so `C#1` addresses `C#1` and not `C`. Values that cannot travel as
  one segment (empty, `.`, `..`, a slash, a backslash, control characters) raise the new `AioticIdentifierError`
  before any request is made.
- `orders.upload()` sends a `request_id` on every call, generating one when you do not pass it, so a request that is
  repeated after a lost response creates no second order. File objects are read once, so a repeated request carries
  the complete body. Requests the API does not declare idempotent (`upload_raw_email`, `orders.retry`,
  `rejected.reprocess`, `erp.send`, `mailbox.fetch_all`) are repeated only when the request provably was not
  processed: connection failures before sending and HTTP 408, 425, 429. A 502, 503 or 504 on those requests is
  surfaced, because an intermediary can answer it after the backend accepted the request. GET, PUT, DELETE and
  `orders.upload` keep the full retry policy.
- `build_app()` refuses to start without `AIOTIC_ERP_RECEIVE_KEY`; the placeholder key is gone. `POST /erp/events`
  exists only when a sync engine is configured and always requires a key: `AIOTIC_ERP_EVENTS_KEY`, falling back to
  the receive key. `create_webhook_routers()` raises when `on_change_events` is passed without `erp_events_key`.
- One `ErpReceiver` instance handles deliveries with the same `request_id` one at a time, so concurrent
  duplicates to that receiver book one order. The `ErpPort` contract now says what the ERP must provide for every
  other case: `create_sales_order` must be safe to call twice with the same `request_id` (a unique external
  reference). `FunctionalApiAdapter` treats HTTP 409 as "already booked" and returns the existing order;
  `DataApiAdapter` resolves a unique-constraint conflict the same way.
- `aiotic sync …` validates its input before anything is sent: comma- and semicolon-separated CSV files (with or
  without a byte order mark) are read correctly — a one-row comma file used to parse as empty — required columns are
  checked per kind, a header with duplicate names (after trimming, ignoring case), a row whose width differs from
  the header or malformed quoting (an unterminated quoted field, characters after a closing quote) is refused, and
  malformed JSON, an unexpected JSON shape or a record with a duplicate key is a usage error instead of an empty or
  reshaped data set. Correctly quoted multi-line values, delimiters and quotes inside quotes work as before.
- `SyncEngine.reconcile()` refuses to delete when a data set is empty while records of that kind were sent earlier;
  the report carries one failure that says so. `allow_empty=True` (CLI: `--allow-empty`) is the explicit opt-in.
- `SyncEngine.apply_many()` applies the events of one record in the order given and never concurrently, and keeps
  the dependencies between records: a mapping after the customer and product events before it, a customer or
  product delete after the mapping events before it; independent events run in parallel, parents first for upserts
  and children first for deletes. A delete followed by a re-create ends with the record present, and a batch that
  creates and then removes a customer, a product and their mapping succeeds in source order. Report counters are
  exact under concurrency.
- `Product` responses accept a missing or null `description`, as the API allows; `ProductUpsert` still requires
  one. `bootstrap_state_from_aiotic()` skips such products instead of failing on the whole page.

### Changed

- Package metadata: links to the changelog, the source repository and the issue tracker (shown on PyPI).
- `AioticTransportError` and `AioticIdentifierError` are exported from the package root.
- README: the package installs from PyPI; the wheel and the sdist stay attached to every GitHub release.
- Releases are published to PyPI by this repository's `publish-pypi` workflow (trusted publishing, the files of the
  GitHub release).
- README: a quick start with the first call from Python and links to the guide.

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
