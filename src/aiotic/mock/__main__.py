import os

import uvicorn

from .app import create_mock_app

if __name__ == "__main__":
    uvicorn.run(
        create_mock_app(
            erp_url=os.environ.get("MOCK_ERP_URL"),
            erp_key=os.environ.get("MOCK_ERP_KEY"),
            webhook_url=os.environ.get("MOCK_WEBHOOK_URL"),
            webhook_key=os.environ.get("MOCK_WEBHOOK_KEY"),
            processing_seconds=float(os.environ.get("MOCK_PROCESSING_SECONDS", "3")),
        ),
        host=os.environ.get("MOCK_HOST", "0.0.0.0"),
        port=int(os.environ.get("MOCK_PORT", "8080")),
    )
