# aiotic-sdk (Python)

The official Python toolkit for integrating **AIOTIC** purchase-order processing with your ERP or business
software.

- **Documentation:** <https://developers.aiotic.ai> — the AIOTIC Integrator Guide, with the SDK chapters, the API
  reference and the contracts for the ERP receive endpoint and the processing webhook.
- **Changes:** [CHANGELOG.md](CHANGELOG.md).
- **Questions and problems:** open an issue in this repository.

## Install

```bash
pip install "aiotic-sdk[all]"        # client + FastAPI service + CLI
pip install aiotic-sdk               # client and validation pipeline only (httpx, pydantic)
```

Python 3.11 or newer. Until the package is on PyPI, install the wheel attached to the latest
[GitHub release](https://github.com/devopscompanynl/aiotic-sdk-python/releases), for example:

```bash
pip install "aiotic-sdk[all] @ https://github.com/devopscompanynl/aiotic-sdk-python/releases/download/vX.Y.Z/aiotic_sdk-X.Y.Z-py3-none-any.whl"
```

## Five commands to a running integration

```bash
aiotic init                          # writes .env + service.py
aiotic mock &                        # a local AIOTIC tenant on :8080 (keys: mock-integration-key / mock-sync-key)
aiotic doctor                        # connectivity, keys, master data present?
aiotic serve                         # your receive endpoint + webhooks on :9000
```

## What is inside

| Module | Purpose |
|---|---|
| `aiotic.AioticClient` / `AsyncAioticClient` | Typed client for every public endpoint, retries, backoff, client-side rate limiting, sync-key routing |
| `aiotic.models` | Pydantic models pinned to the public OpenAPI document |
| `aiotic.receive` | The ERP receive endpoint: API-key check, idempotency, pipeline, correct `{success, …}` response |
| `aiotic.pipeline` | Sanitizers → validators → business rules — the validation layer for ERPs with only a data API |
| `aiotic.erp` | `ErpPort` + templates: `FunctionalApiAdapter`, `DataApiAdapter`, `InMemoryErp` |
| `aiotic.sync` | Event-driven master-data sync with hash-based reconciliation (never a full re-upload) |
| `aiotic.watch` | `OrderWatcher`: polls order status and emits transitions |
| `aiotic.webhooks` | Receivers for AIOTIC's processing webhook and your ERP's change events |
| `aiotic.service` | `build_app()` — a complete integration service in one call |
| `aiotic.mock` | A mock AIOTIC tenant for local development and CI |

## Development

```bash
uv sync --all-extras
uv run pytest -q
```

This repository is published release by release from the AIOTIC documentation sources, one commit per release.
Pull requests cannot be merged here directly; please open an issue and we will take the change into the next release.

## License

MIT, see [LICENSE](LICENSE).
