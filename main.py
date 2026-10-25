"""Entrypoint for builders that look for main.py at the repo root.

The service is app/__main__.py; this only starts it, so there is one
implementation and not two.
"""

import os

import uvicorn

from app.__main__ import app

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
