"""Built-in sanitizers. Each returns a *new* order; the original is never mutated."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from typing import Any

from ..models import ErpPurchaseOrder
from . import Context

_COUNTRY_ALIASES = {
    "NEDERLAND": "NL", "NETHERLANDS": "NL", "THE NETHERLANDS": "NL", "HOLLAND": "NL", "NLD": "NL",
    "DEUTSCHLAND": "DE", "GERMANY": "DE", "DEU": "DE",
    "BELGIË": "BE", "BELGIE": "BE", "BELGIQUE": "BE", "BELGIUM": "BE", "BEL": "BE",
    "FRANCE": "FR", "FRA": "FR", "ÖSTERREICH": "AT", "OESTERREICH": "AT", "AUSTRIA": "AT", "AUT": "AT",
    "SCHWEIZ": "CH", "SUISSE": "CH", "SWITZERLAND": "CH", "CHE": "CH",
    "UNITED KINGDOM": "GB", "GREAT BRITAIN": "GB", "UK": "GB", "GBR": "GB",
    "ESPAÑA": "ES", "SPAIN": "ES", "ESP": "ES", "ITALIA": "IT", "ITALY": "IT", "ITA": "IT",
    "POLSKA": "PL", "POLAND": "PL", "POL": "PL", "DANMARK": "DK", "DENMARK": "DK", "DNK": "DK",
    "SVERIGE": "SE", "SWEDEN": "SE", "SWE": "SE", "NORGE": "NO", "NORWAY": "NO", "NOR": "NO",
    "LUXEMBOURG": "LU", "LUXEMBURG": "LU", "LUX": "LU",
}


def _walk_strings(obj: Any, fn: Callable[[str], str]) -> Any:
    if isinstance(obj, str):
        return fn(obj)
    if isinstance(obj, list):
        return [_walk_strings(v, fn) for v in obj]
    if isinstance(obj, dict):
        return {k: _walk_strings(v, fn) for k, v in obj.items()}
    return obj


class StripWhitespace:
    """Trim every string field and collapse internal runs of whitespace. Empty strings become ``None``."""

    name = "strip_whitespace"

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        def clean(s: str) -> Any:
            s2 = re.sub(r"\s+", " ", s).strip()
            return s2 if s2 else None

        data = _walk_strings(order.model_dump(mode="python"), clean)
        if data.get("order_number") is None:
            data["order_number"] = order.order_number
        return ErpPurchaseOrder.model_validate(data)


class NormalizeCountryCodes:
    """Turn country names / alpha-3 codes into ISO 3166-1 alpha-2 (``Nederland`` → ``NL``)."""

    name = "normalize_country_codes"

    def __init__(self, extra: Mapping[str, str] | None = None, default: str | None = None):
        self.aliases = {**_COUNTRY_ALIASES, **{k.upper(): v for k, v in (extra or {}).items()}}
        self.default = default

    def _norm(self, value: str | None) -> str | None:
        if value is None:
            return self.default
        v = value.strip().upper()
        if len(v) == 2 and v.isalpha():
            return v
        return self.aliases.get(v, value if len(v) != 3 else v[:2]) if v else self.default

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        o.customer.address.country = self._norm(o.customer.address.country)
        o.shipping_details.recipient.address.country = self._norm(o.shipping_details.recipient.address.country)
        if o.supplier and o.supplier.address:
            o.supplier.address.country = self._norm(o.supplier.address.country)
        return o


class NormalizeCurrency:
    """Uppercase currency codes and map symbols (``€`` → ``EUR``); fill missing line currencies from the header."""

    name = "normalize_currency"
    _symbols = {"€": "EUR", "EUR.": "EUR", "$": "USD", "US$": "USD", "£": "GBP", "CHF.": "CHF"}

    def __init__(self, default: str | None = "EUR"):
        self.default = default

    def _norm(self, value: str | None) -> str | None:
        if not value:
            return None
        v = value.strip().upper()
        return self._symbols.get(v, v)

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        o.currency = self._norm(o.currency) or self.default
        for item in o.items:
            item.currency = self._norm(item.currency) or o.currency
        return o


class NormalizeOrderNumber:
    """Strip characters your ERP cannot store in a reference field (AIOTIC already turns ``/`` into ``-``)."""

    name = "normalize_order_number"

    def __init__(self, pattern: str = r"[^A-Za-z0-9._\-]+", replacement: str = "-", max_length: int | None = 35):
        self.pattern = re.compile(pattern)
        self.replacement = replacement
        self.max_length = max_length

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        n = self.pattern.sub(self.replacement, o.order_number).strip(self.replacement)
        o.order_number = n[: self.max_length] if self.max_length else n
        return o


class MapUnits:
    """Map units of measure as printed on documents to your ERP's codes (``stuks``/``Stk``/``pcs`` → ``ST``)."""

    name = "map_units"
    _default = {
        "ST": "ST", "STK": "ST", "STUKS": "ST", "STUK": "ST", "PCS": "ST", "PC": "ST", "PIECE": "ST", "PIECES": "ST",
        "EA": "ST", "EACH": "ST", "PCE": "ST", "STÜCK": "ST", "STUECK": "ST",
        "M": "M", "MTR": "M", "METER": "M", "METERS": "M", "MTRS": "M", "LM": "M",
        "KG": "KG", "KILO": "KG", "L": "L", "LTR": "L", "LITER": "L",
        "DOOS": "BOX", "BOX": "BOX", "KARTON": "BOX", "CTN": "BOX", "PAK": "PK", "PACK": "PK", "PK": "PK",
        "ROL": "ROL", "ROLL": "ROL", "SET": "SET", "PAAR": "PR", "PAIR": "PR", "PR": "PR",
    }

    def __init__(self, mapping: Mapping[str, str] | None = None, *, default_unit: str | None = "ST", keep_unknown: bool = True):
        self.mapping = {**self._default, **{k.upper(): v for k, v in (mapping or {}).items()}}
        self.default_unit = default_unit
        self.keep_unknown = keep_unknown

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        for item in o.items:
            if not item.unit:
                item.unit = self.default_unit
                continue
            key = item.unit.strip().upper().rstrip(".")
            item.unit = self.mapping.get(key, item.unit if self.keep_unknown else self.default_unit)
        return o


class NormalizePostalCodes:
    """Uppercase and space Dutch postal codes (``1234ab`` → ``1234 AB``); trims others."""

    name = "normalize_postal_codes"
    _nl = re.compile(r"^(\d{4})\s*([A-Za-z]{2})$")

    def _norm(self, value: str | None, country: str | None) -> str | None:
        if not value:
            return value
        v = value.strip().upper()
        m = self._nl.match(v)
        if m and (country in (None, "NL")):
            return f"{m.group(1)} {m.group(2)}"
        return v

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        ca, ra = o.customer.address, o.shipping_details.recipient.address
        ca.postal_code = self._norm(ca.postal_code, ca.country)
        ra.postal_code = self._norm(ra.postal_code, ra.country)
        return o


class DropEmptyLines:
    """Remove lines that have neither a quantity nor an article number (pure noise rows)."""

    name = "drop_empty_lines"

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        o.items = [i for i in o.items if (i.quantity or 0) > 0 or i.article_number or i.price is not None]
        return o


class FillShippingFromCustomer:
    """When the shipping recipient is empty, copy the customer block (common for forwarded orders)."""

    name = "fill_shipping_from_customer"

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        o = order.model_copy(deep=True)
        r = o.shipping_details.recipient
        if not any([r.company, r.address.street, r.address.postal_code, r.address.city]):
            c = o.customer
            r.company = c.company
            r.contact_person = c.contact_person
            r.email, r.phone = c.email, c.phone
            r.address = c.address.model_copy()
        return o


class Custom:
    """Wrap any ``(order, ctx) -> order`` function as a sanitizer."""

    def __init__(self, name: str, fn: Callable[[ErpPurchaseOrder, Context], ErpPurchaseOrder]):
        self.name = name
        self.fn = fn

    def apply(self, order: ErpPurchaseOrder, ctx: Context) -> ErpPurchaseOrder:
        return self.fn(order, ctx)
