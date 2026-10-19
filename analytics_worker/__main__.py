"""Run with python -m analytics_worker; release dependencies on shutdown."""

import asyncio
import logging
import signal

from analytics_worker.bootstrap import create_worker


async def main() -> None:
    loop = asyncio.get_running_loop()
    task = asyncio.current_task()
    registered = []
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, task.cancel)
            registered.append(signum)
        except NotImplementedError:
            # Windows asyncio.run already translates Ctrl+C to cancellation.
            pass
    try:
        async with create_worker() as worker:
            await worker.run()
    finally:
        for signum in registered:
            loop.remove_signal_handler(signum)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
