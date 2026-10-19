"""Run the HTTP service: python -m inventory_api."""

import logging
import os

from aiohttp import web

from inventory_api import create_app


def main():
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    web.run_app(
        create_app(), host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8080"))
    )


if __name__ == "__main__":
    main()
