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

Python 3.11 or newer. Every release is also attached to the
[GitHub releases](https://github.com/devopscompanynl/aiotic-sdk-python/releases) as a wheel and an sdist.

## Quick start

The full walkthrough, copy-paste ready, runs a mock AIOTIC tenant, a receive endpoint and a demo ERP in about
15 minutes: **[Quick start in the Integrator Guide](https://developers.aiotic.ai/guide/quick-start)**. The short
version:

```bash
pip install "aiotic-sdk[all]"
aiotic mock &                        # a local AIOTIC tenant on :8080 (keys: mock-integration-key / mock-sync-key)
aiotic init                          # asks for the tenant URL and key, writes .env + service.py
aiotic doctor                        # connectivity, keys, master data present?
aiotic serve                         # your receive endpoint + webhooks on :9000
```

Your first call from Python, with the `.env` that `aiotic init` wrote:

```python
from aiotic import AioticClient

with AioticClient() as client:                        # reads AIOTIC_BASE_URL and AIOTIC_API_KEY from .env or the environment
    uploaded = client.orders.upload(["PO-4711.pdf"])  # one purchase order, one or more files
    order = client.orders.wait(uploaded.request_id)   # polls until AIOTIC has processed it
    print(order.status, order.request_id)
```

Next steps in the guide: [SDK overview](https://developers.aiotic.ai/sdk/overview),
[bootstrapping a service](https://developers.aiotic.ai/sdk/bootstrapping),
[client reference](https://developers.aiotic.ai/sdk/client).

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
