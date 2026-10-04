"""The integration-layer pipeline: sanitizers → validators → business rules.

Why this exists
---------------
Some ERPs expose a *functional* API that validates a sales order before it is created. Others
expose only a *data* API (or a database) where whatever you write lands unvalidated and blows up
later when the ERP starts using the record. For those, the integration layer must do the work
the ERP would have done. The pipeline makes that explicit, testable and easy to extend:

* :class:`Sanitizer` — returns a cleaned copy of the order (trim, normalise codes, map units, …).
* :class:`Validator` — yields :class:`Issue` objects about *data shape* (required fields, catalog
  membership, totals). Errors reject the order.
* :class:`BusinessRule` — yields :class:`Issue` objects about *policy* (credit hold, duplicates,
  cut-off dates). Same contract as a validator; separated only for readability.

A :class:`Pipeline` runs all steps and returns a :class:`Verdict`. In the ERP receive endpoint a
rejecting verdict becomes ``{"success": false, "error": "…"}`` which AIOTIC shows to the operator.

    from aiotic.pipeline import Pipeline, sanitizers as S, validators as V, rules as R

    pipeline = Pipeline(
        sanitizers=[S.StripWhitespace(), S.NormalizeCountryCodes(), S.MapUnits({"stuks": "ST"})],
        validators=[V.RequiredFields(), V.CustomerResolved(), V.ArticlesInCatalog(catalog), V.PositiveQuantities()],
        rules=[R.NoDuplicateOrder(store), R.CustomerNotBlocked(customers)],
    )
    verdict = pipeline.run(order, context={"request_id": request_id})
    if not verdict.ok:
        return ErpReceiveResponse.rejected(verdict.error_message())
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from ..models import ErpPurchaseOrder

log = logging.getLogger("aiotic.pipeline")


class Severity(StrEnum):
    ERROR = "error"  # rejects the order
    WARNING = "warning"  # recorded, order still accepted
    INFO = "info"


@dataclass(slots=True)
class Issue:
    code: str
    message: str
    severity: Severity = Severity.ERROR
    path: str | None = None  # e.g. "items[2].article_number"
    data: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.message}" + (f" ({self.path})" if self.path else "")


@dataclass(slots=True)
class Context:
    """Read/write bag handed to every step (request id, tenant, cached lookups, …)."""

    request_id: str | None = None
    values: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.values.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.values[key] = value


@runtime_checkable
class Sanitizer(Protocol):
    name: str

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder: ...


@runtime_checkable
class Validator(Protocol):
    name: str

    def check(self, order: ErpPurchaseOrder, ctx: Context) -> Iterable[Issue]: ...


BusinessRule = Validator  # same contract; separate list in the pipeline for readability


@dataclass(slots=True)
class Verdict:
    order: ErpPurchaseOrder
    issues: list[Issue]
    steps: list[str]

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]

    @property
    def ok(self) -> bool:
        return not self.errors

    def error_message(self, limit: int = 5) -> str:
        """One human-readable line for the operator (AIOTIC displays it verbatim)."""
        msgs = [str(i) for i in self.errors[:limit]]
        more = len(self.errors) - limit
        return "; ".join(msgs) + (f"; and {more} more" if more > 0 else "")


class Pipeline:
    """Runs sanitizers, then validators, then business rules. Steps are plain objects — add your own."""

    def __init__(
        self,
        *,
        sanitizers: Sequence[Sanitizer] = (),
        validators: Sequence[Validator] = (),
        rules: Sequence[BusinessRule] = (),
        stop_on_first_error: bool = False,
    ):
        self.sanitizers = list(sanitizers)
        self.validators = list(validators)
        self.rules = list(rules)
        self.stop_on_first_error = stop_on_first_error

    def run(self, order: ErpPurchaseOrder, context: Context | dict[str, Any] | None = None) -> Verdict:
        ctx = context if isinstance(context, Context) else Context(values=dict(context or {}))
        if isinstance(context, dict) and "request_id" in context:
            ctx.request_id = str(context["request_id"])
        issues: list[Issue] = []
        steps: list[str] = []
        current = order.model_copy(deep=True)
        for s in self.sanitizers:
            current = s.apply(current, ctx)
            steps.append(f"sanitize:{s.name}")
        for group, checks in (("validate", self.validators), ("rule", self.rules)):
            for v in checks:
                found = list(v.check(current, ctx))
                issues.extend(found)
                steps.append(f"{group}:{v.name}")
                if self.stop_on_first_error and any(i.severity == Severity.ERROR for i in found):
                    return Verdict(current, issues, steps)
        verdict = Verdict(current, issues, steps)
        if not verdict.ok:
            log.info("pipeline rejected order %s: %s", ctx.request_id, verdict.error_message())
        return verdict


from . import rules, sanitizers, validators  # noqa: E402  (re-exported for convenience)

__all__ = [
    "Severity",
    "Issue",
    "Context",
    "Sanitizer",
    "Validator",
    "BusinessRule",
    "Verdict",
    "Pipeline",
    "sanitizers",
    "validators",
    "rules",
]
