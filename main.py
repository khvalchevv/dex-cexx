"""dex-cex entry point — WS CEX feeds + currency cache + token universe +
DexScreener DEX feed + CEX-CEX & CEX-DEX scanner + Telegram alerter."""
import asyncio
import io
import logging
import os
import signal
import sys
from dotenv import load_dotenv

load_dotenv()

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("main")

from exchanges import init_all, close_all
from cex_watcher import CexPriceBook, watch_exchange_tickers
from scanner import scan_loop
from alerter import TelegramAlerter
from currencies import CurrencyCache
from blacklist import TokenBlacklist
from universe import TokenUniverse, _norm as _norm_eid
from dex_watcher import DexWatcher, DexPriceBook
from dex_verify import DexVerifier


class _SuppressFutureNoise(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "Future exception was never retrieved" in msg:
            return False
        if "Unclosed client session" in msg:
            return False
        return True


logging.getLogger("asyncio").addFilter(_SuppressFutureNoise())

MIN_SPREAD = float(os.getenv("MIN_SPREAD_PCT", "6.0"))
SCAN_INTERVAL = int(os.getenv("SCAN_INTERVAL_SEC", "5"))
MAX_AGE = int(os.getenv("MAX_PRICE_AGE_SEC", "30"))
COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))


async def _first_currency_pass(currency_cache: CurrencyCache, exchanges: dict):
    """One synchronous full currency refresh so the universe can be built."""
    seen = set()
    tasks = []
    for eid, inst in exchanges.items():
        nid = _norm_eid(eid)
        if nid in seen:
            continue
        seen.add(nid)
        tasks.append(currency_cache.refresh_one(eid, inst))
    await asyncio.gather(*tasks, return_exceptions=True)
    total = sum(len(v) for v in currency_cache.cache.values())
    log.info("initial currency pass: %d exchanges, %d assets",
             len(currency_cache.cache), total)


async def main() -> None:
    cex_book = CexPriceBook()
    stop = asyncio.Event()

    def _sig_handler(*_):
        log.info("stop signal received")
        stop.set()
    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for s in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(s, _sig_handler)

    log.info("starting (CEX-CEX): min_spread=%.1f%%, scan=%ds, max_age=%ds",
             MIN_SPREAD, SCAN_INTERVAL, MAX_AGE)

    exchanges = await init_all()
    if not exchanges:
        log.error("no exchanges active — exit")
        return

    currency_cache = CurrencyCache()
    blacklist = TokenBlacklist()
    universe = TokenUniverse()
    dex_book = DexPriceBook()
    verifier = DexVerifier()
    alerter = TelegramAlerter(currency_cache=currency_cache, blacklist=blacklist)
    await alerter.start()

    # Universe: use fresh on-disk copy if available, else build from a
    # synchronous currency pass.
    if not universe.load():
        log.info("building universe from scratch (one currency pass)...")
        await _first_currency_pass(currency_cache, exchanges)
        universe.build_from_cache(currency_cache, currency_cache.names_snapshot())
        universe.save()

    tasks = [
        asyncio.create_task(watch_exchange_tickers(eid, inst, cex_book, stop),
                             name=f"ws-{eid}")
        for eid, inst in exchanges.items()
    ]
    tasks.append(asyncio.create_task(
        currency_cache.refresh_loop(exchanges, stop), name="currency-cache"))
    tasks.append(asyncio.create_task(
        universe.rebuild_loop(currency_cache,
                               currency_cache.names_snapshot, stop),
        name="universe-rebuild"))
    tasks.append(asyncio.create_task(
        DexWatcher(universe, dex_book).run(stop), name="dex-watcher"))
    tasks.append(asyncio.create_task(
        scan_loop(cex_book, universe, alerter.send,
                   min_spread_pct=MIN_SPREAD,
                   interval_sec=SCAN_INTERVAL,
                   max_age_sec=MAX_AGE,
                   cooldown_sec=COOLDOWN,
                   blacklist=blacklist,
                   dex_book=dex_book,
                   verifier=verifier,
                   stop_event=stop),
        name="scanner"))

    try:
        await stop.wait()
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt")
    finally:
        log.info("shutting down...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await alerter.stop()
        await verifier.close()
        await close_all(exchanges)
        log.info("done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
