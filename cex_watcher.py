"""CEX side: 1 watch_tickers per exchange feeds a global PriceBook keyed by token symbol.
Ported from ws-arb/watcher.py with light naming changes (PriceBook -> CexPriceBook)."""
import asyncio
import logging
import time
import ccxt.pro as ccxtpro

log = logging.getLogger(__name__)


class CexPriceBook:
    """Map: token_symbol (base) -> {exchange_id: {pair, bid, ask, ts}}.
    Only spot, only USD-stable quotes."""

    QUOTE_WHITELIST = {"USDT", "USDC", "USD", "BUSD", "DAI", "FDUSD"}

    def __init__(self):
        self.book: dict[str, dict[str, dict]] = {}
        self.lock = asyncio.Lock()

    async def update(self, exch_id: str, pair: str, bid: float | None, ask: float | None):
        if not bid or not ask or bid <= 0 or ask <= bid:
            return
        if "/" not in pair:
            return
        base, _, quote = pair.partition("/")
        if quote not in self.QUOTE_WHITELIST:
            return
        norm_eid = exch_id.partition("#")[0]  # collapse bingx#0..N -> bingx
        async with self.lock:
            self.book.setdefault(base, {})[norm_eid] = {
                "pair": pair, "bid": float(bid), "ask": float(ask),
                "ts": time.time(),
            }

    async def snapshot(self) -> dict[str, dict[str, dict]]:
        async with self.lock:
            return {tok: dict(exs) for tok, exs in self.book.items()}

    async def tokens(self) -> list[str]:
        async with self.lock:
            return list(self.book.keys())


# Per-exchange WS subscription caps. Empirical limits to stay under each venue's
# documented max-subscriptions-per-connection. Smaller is also fine but reduces coverage.
EXCHANGE_MAX_SYMBOLS = {
    "kucoin": 100,       # documented: 100 topics per WS connection
    "okx": 200,          # documented: 240
    "binance": 500,      # documented: 1024 streams per single connection
    "bybit": 10,         # bybit watch_tickers rejects args >10 — small batch only
    "mexc": 100,         # mexc throttles aggressively
    "gateio": 300,
    "bitget": 200,
    "phemex": 100,
    "bitmart": 20,       # ccxt-enforced: max 20 symbols per watch_tickers request
    "p2b": 80,
}
DEFAULT_MAX_SYMBOLS = 500

# Exchanges where per-symbol watch_ticker multiplex on a single WS works
# cleanly (empirically verified — see attempt log for the ones that failed).
#
# Rejected after live test:
#   mexc    — Connection closed by remote server (sub cap ~30 hard)
#   bitmart — "Subscribed total topic quantity exceed" and rate-limit storm
#   bitrue  — ccxt.pro raises "watchTicker() is not supported yet"
#   kucoin  — Connection timeout under load
#   lbank   — Connection timeout storm (1700+ errs/min under load)
# These fall back to bulk watch_tickers with universe-filtered cap.
UNIVERSE_MULTIPLEX_EIDS = {
    "bybit", "phemex", "p2b",
}


def _build_symbol_list(inst, exch_id: str = "",
                        universe_tickers: set[str] | None = None) -> list[str]:
    """Return list of USD-quoted spot symbols on the instance.

    When universe_tickers is provided, filter to pairs whose BASE is in the
    universe — this keeps the sub set tight (only tokens we care about) and
    lets multi-thousand-pair exchanges (mexc/lbank/bitrue) stay within cap
    while still covering every universe token they list."""
    syms = []
    for sym, m in (inst.markets or {}).items():
        if not m.get("spot"):
            continue
        # `active` may be None for some exchanges (coinex, exmo) where the field
        # isn't populated — accept None as "not explicitly disabled".
        if m.get("active") is False:
            continue
        q = m.get("quote", "")
        if q not in ("USDT", "USDC", "USD", "BUSD", "DAI", "FDUSD"):
            continue
        if universe_tickers is not None:
            base = sym.partition("/")[0].upper()
            if base not in universe_tickers:
                continue
        syms.append(sym)
    # Prefer USDT pairs first so the cap keeps the most-liquid markets
    syms.sort(key=lambda s: 0 if s.endswith("/USDT") else 1)
    # Only apply cap to bulk-mode exchanges. Multiplex-mode uses per-symbol
    # WS which multiplexes on one WS connection — the cap doesn't apply.
    base_eid = exch_id.partition("#")[0]
    if base_eid not in UNIVERSE_MULTIPLEX_EIDS:
        cap = EXCHANGE_MAX_SYMBOLS.get(exch_id, DEFAULT_MAX_SYMBOLS)
        syms = syms[:cap]
    return syms


async def _watch_per_symbol(exch_id: str, inst: ccxtpro.Exchange, symbols: list[str],
                             book: CexPriceBook, stop_event: asyncio.Event) -> None:
    async def _one(sym: str):
        while not stop_event.is_set():
            try:
                t = await inst.watch_ticker(sym)
                await book.update(exch_id, sym, t.get("bid"), t.get("ask"))
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("[%s] %s WS err: %s", exch_id, sym, str(e)[:60])
                await asyncio.sleep(5)
    await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=True)


async def _watch_per_symbol_orderbook(exch_id: str, inst: ccxtpro.Exchange,
                                       symbols: list[str], book: CexPriceBook,
                                       stop_event: asyncio.Event) -> None:
    """Per-symbol watch_order_book fallback. All symbols multiplex on a single
    WS connection of the same instance.

    Robustness layer:
      - Per-exchange aggregated error counter (avoids per-symbol log flood when
        the whole WS endpoint is unreachable).
      - Circuit breaker: if the exchange yields zero successful messages within
        CIRCUIT_BREAKER_SEC, stop subscribing — keeps logs clean and frees CPU.
    """
    # Shared per-exchange counters
    stats = {"ok": 0, "err": 0, "last_log": time.monotonic(), "first_err": None}
    started_at = time.monotonic()

    async def _one(sym: str):
        while not stop_event.is_set():
            try:
                ob = await inst.watch_order_book(sym)
                bids = ob.get("bids") or []
                asks = ob.get("asks") or []
                bid = bids[0][0] if bids else None
                ask = asks[0][0] if asks else None
                await book.update(exch_id, sym, bid, ask)
                stats["ok"] += 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                stats["err"] += 1
                if stats["first_err"] is None:
                    stats["first_err"] = str(e)[:120]
                # Periodic aggregated log (every 60s), not per symbol
                now = time.monotonic()
                if now - stats["last_log"] > 60:
                    log.warning("[%s] OB stats: ok=%d err=%d (last err: %s)",
                                 exch_id, stats["ok"], stats["err"], stats["first_err"])
                    stats["last_log"] = now
                await asyncio.sleep(5)

    async def _watchdog():
        """Kill the subscription if no message arrived within CIRCUIT_BREAKER_SEC."""
        while not stop_event.is_set():
            await asyncio.sleep(30)
            elapsed = time.monotonic() - started_at
            if stats["ok"] == 0 and elapsed > CIRCUIT_BREAKER_SEC:
                log.warning("[%s] dead WS — no messages in %ds, giving up "
                            "(err count: %d, first: %s)",
                            exch_id, int(elapsed), stats["err"], stats["first_err"])
                raise asyncio.CancelledError()

    try:
        await asyncio.gather(
            *[_one(s) for s in symbols],
            _watchdog(),
            return_exceptions=False,
        )
    except asyncio.CancelledError:
        pass


# Cap symbols when falling back to per-symbol orderbook subscriptions —
# 100 streams per WS connection is comfortable for most exchanges.
ORDERBOOK_FALLBACK_MAX_SYMBOLS = 100
CIRCUIT_BREAKER_SEC = 120  # bail on the exchange if no successful frame in this window


# Cap symbols when falling back to per-symbol orderbook subscriptions —
# 100 streams per WS connection is comfortable for most exchanges.
ORDERBOOK_FALLBACK_MAX_SYMBOLS = 100


async def watch_exchange_tickers(exch_id: str, inst: ccxtpro.Exchange,
                                  book: CexPriceBook, stop_event: asyncio.Event,
                                  universe_tickers: set[str] | None = None) -> None:
    if exch_id.startswith("bingx#"):
        symbols = getattr(inst, "_shard_symbols", [])
        if universe_tickers is not None:
            symbols = [s for s in symbols
                       if s.partition("/")[0].upper() in universe_tickers]
        log.info("[%s] per-symbol WS on %d pairs", exch_id, len(symbols))
        await _watch_per_symbol(exch_id, inst, symbols, book, stop_event)
        return

    symbols = _build_symbol_list(inst, exch_id, universe_tickers)
    if not symbols:
        log.warning("[%s] no spot/USD pairs -- REST polling", exch_id)
        await rest_poll(exch_id, inst, book, stop_event)
        return

    base_eid = exch_id.partition("#")[0]
    # Multiplex mode: per-symbol WS on one connection. Bypasses bulk caps.
    if base_eid in UNIVERSE_MULTIPLEX_EIDS:
        log.info("[%s] universe multiplex: %d pairs on per-symbol WS",
                 exch_id, len(symbols))
        await _watch_per_symbol(exch_id, inst, symbols, book, stop_event)
        return

    log.info("[%s] WS subscribe to %d pairs", exch_id, len(symbols))

    has_bulk = inst.has.get("watchTickers")
    has_ob = inst.has.get("watchOrderBook")

    if not has_bulk and has_ob:
        # No bulk ticker stream — use per-symbol orderbook (cheaper than per-symbol ticker
        # because we only read top-of-book; multiplexed on one WS connection).
        ob_symbols = symbols[:ORDERBOOK_FALLBACK_MAX_SYMBOLS]
        log.info("[%s] orderbook fallback on %d pairs", exch_id, len(ob_symbols))
        await _watch_per_symbol_orderbook(exch_id, inst, ob_symbols, book, stop_event)
        return

    if not has_bulk and not has_ob:
        log.warning("[%s] no usable WS -- REST polling", exch_id)
        await rest_poll(exch_id, inst, book, stop_event)
        return

    while not stop_event.is_set():
        try:
            tickers = await inst.watch_tickers(symbols)
            for pair, t in tickers.items():
                await book.update(exch_id, pair, t.get("bid"), t.get("ask"))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err = str(e)[:120]
            if "not supported" in err or "does not have market" in err:
                if has_ob:
                    log.warning("[%s] watchTickers failed (%s) -- orderbook fallback",
                                exch_id, err[:60])
                    await _watch_per_symbol_orderbook(
                        exch_id, inst, symbols[:ORDERBOOK_FALLBACK_MAX_SYMBOLS],
                        book, stop_event)
                else:
                    log.warning("[%s] WS unsupported (%s) -- REST polling", exch_id, err[:60])
                    await rest_poll(exch_id, inst, book, stop_event)
                return
            log.warning("[%s] WS error: %s -- retry 5s", exch_id, err)
            await asyncio.sleep(5)


async def rest_poll(exch_id: str, inst, book: CexPriceBook,
                    stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            tickers = await inst.fetch_tickers()
            for pair, t in tickers.items():
                await book.update(exch_id, pair, t.get("bid"), t.get("ask"))
        except Exception as e:
            log.warning("[%s] REST poll err: %s", exch_id, str(e)[:80])
        await asyncio.sleep(30)
