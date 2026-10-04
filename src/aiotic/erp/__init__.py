"""ERP adapters. Implement :class:`ErpPort` for your system; two templates are included:

* :class:`aiotic.erp.functional_api.FunctionalApiAdapter` — the ERP has an API that validates and
  creates sales orders (Business Central, Exact Online, Odoo, SAP B1 Service Layer, …).
* :class:`aiotic.erp.data_api.DataApiAdapter` — the ERP only exposes tables (a data API or the
  database). Everything the ERP would have validated has to be done in the pipeline first.

:class:`aiotic.erp.memory.InMemoryErp` is a complete fake used by the tests and the quick start.
"""

from .ports import CatalogPort, CustomerPort, ErpCreateResult, ErpPort

__all__ = ["ErpPort", "CatalogPort", "CustomerPort", "ErpCreateResult"]
