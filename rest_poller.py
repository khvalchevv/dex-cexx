"""Per-exchange bookTicker REST poller — direct aiohttp calls, no ccxt.

Each exchange has a dedicated bulk bookTicker endpoint returning ONLY
{symbol, bid, ask} for all pairs — 200-500KB, 800-1500ms round-trip vs
`fetch_tickers()` which returns full ticker (last, high, low, vol, etc)
and takes 5-15s. Perfect for feeding a live price cache.

Writes bid/ask into the shared CexPriceBook keyed by base symbol,
filtered to universe tokens. Each exchange has a bespoke URL and
parser — that complexity lives here (small, isolated) so the rest of
the bot doesn't care.

Proxy strategy: default direct. If direct fails on an exchange
(geo-block from user IP → binance in some regions), that exchange gets
proxied on retry.
"""
import asyncio
import logging
import random
import time
from typing import Callable

import aiohttp

from exchanges import load_proxies

log = logging.getLogger(__name__)

POLL_INTERVAL = 2.0
REQUEST_TIMEOUT = 12.0
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


# ============================================================================
# Per-exchange config: URL + parser -> yields (symbol_pair_str, bid, ask)
# symbol_pair_str is exchange-native (e.g. "DEXEUSDT" for binance) — we
# normalise to "BASE/QUOTE" below by splitting off the trailing quote.
# ============================================================================

_QUOTES = ("USDT", "USDC", "USD", "FDUSD", "BUSD", "DAI")


def _split_pair_concat(sym: str) -> tuple[str, str] | None:
    """'DEXEUSDT' -> ('DEXE','USDT')."""
    for q in _QUOTES:
        if sym.endswith(q) and len(sym) > len(q):
            return sym[:-len(q)], q
    return None


def _p_binance(d):
    # [{"symbol":"BTCUSDT","bidPrice":"...","askPrice":"..."}]
    for r in d:
        s = r.get("symbol", "")
        split = _split_pair_concat(s)
        if split:
            yield split[0], split[1], r.get("bidPrice"), r.get("askPrice")


def _p_mexc(d):
    for r in d:
        s = r.get("symbol", "")
        split = _split_pair_concat(s)
        if split:
            yield split[0], split[1], r.get("bidPrice"), r.get("askPrice")


def _p_okx(d):
    # {"data":[{"instId":"BTC-USDT","bidPx":"...","askPx":"..."}]}
    for r in d.get("data", []):
        inst = r.get("instId", "")
        if "-" in inst:
            base, _, quote = inst.partition("-")
            if quote in _QUOTES:
                yield base, quote, r.get("bidPx"), r.get("askPx")


def _p_gate(d):
    # [{"currency_pair":"BTC_USDT","highest_bid":"...","lowest_ask":"..."}]
    for r in d:
        cp = r.get("currency_pair", "")
        if "_" in cp:
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("highest_bid"), r.get("lowest_ask")


def _p_kucoin(d):
    # {"data":{"ticker":[{"symbol":"BTC-USDT","buy":"...","sell":"..."}]}}
    for r in d.get("data", {}).get("ticker", []):
        s = r.get("symbol", "")
        if "-" in s:
            base, _, quote = s.partition("-")
            if quote in _QUOTES:
                yield base, quote, r.get("buy"), r.get("sell")


def _p_bybit(d):
    # {"result":{"list":[{"symbol":"BTCUSDT","bid1Price":"..","ask1Price":".."}]}}
    for r in d.get("result", {}).get("list", []):
        s = r.get("symbol", "")
        split = _split_pair_concat(s)
        if split:
            yield split[0], split[1], r.get("bid1Price"), r.get("ask1Price")


def _p_bitget(d):
    # {"data":[{"symbol":"BTCUSDT","bidPr":"...","askPr":"..."}]}
    for r in d.get("data", []):
        s = r.get("symbol", "")
        split = _split_pair_concat(s)
        if split:
            yield split[0], split[1], r.get("bidPr"), r.get("askPr")


def _p_bitmart(d):
    # {"data":[["BTC_USDT","<last>","<vol24h>","<open24h>","<high24h>","<low24h>","<change>","<qvol24h>","<bid>","<bidSz>","<ask>","<askSz>","<ts>"]]}
    for r in d.get("data", []):
        if not isinstance(r, list) or len(r) < 11:
            continue
        cp = r[0]
        if "_" in cp:
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r[8], r[10]


def _p_hitbtc(d):
    # {"BTCUSDT":{"bid":"...","ask":"..."}}
    for s, r in d.items():
        split = _split_pair_concat(s)
        if split and isinstance(r, dict):
            yield split[0], split[1], r.get("bid"), r.get("ask")


def _p_whitebit(d):
    # {"BTC_USDT":{"bid":"...","ask":"..."}}
    for cp, r in d.items():
        if "_" in cp and isinstance(r, dict):
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("bid"), r.get("ask")


def _p_coinex(d):
    # {"data":[{"market":"BTCUSDT","best_bid_price":"...","best_ask_price":"..."}]}
    for r in d.get("data", []):
        s = r.get("market", "")
        split = _split_pair_concat(s)
        if split:
            yield split[0], split[1], r.get("best_bid_price"), r.get("best_ask_price")


def _p_xt(d):
    # {"result":[{"s":"btc_usdt","bp":"..","ap":".."}]}
    for r in d.get("result", []):
        cp = (r.get("s") or "").upper()
        if "_" in cp:
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("bp"), r.get("ap")


def _p_lbank(d):
    # {"data":[{"symbol":"btc_usdt","ticker":{"latest":"..","turnover":".."}}]}
    # lbank has a bookTicker endpoint too: /v2/supplement/ticker/bookTicker.do
    # returns {"data":[{"symbol":"btc_usdt","bidPrice":"..","askPrice":".."}]}
    for r in d.get("data", []):
        cp = (r.get("symbol") or "").upper()
        if "_" in cp:
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("bidPrice"), r.get("askPrice")


def _p_bitrue(d):
    # [{"symbol":"BTCUSDT","bidPrice":"..","askPrice":".."}]
    for r in d:
        s = r.get("symbol", "")
        split = _split_pair_concat(s)
        if split:
            yield split[0], split[1], r.get("bidPrice"), r.get("askPrice")


def _p_poloniex(d):
    # [{"symbol":"BTC_USDT","bid":"..","ask":".."}] (some endpoints)
    for r in d:
        s = r.get("symbol", "")
        if "_" in s:
            base, _, quote = s.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("bid"), r.get("ask")


def _p_cryptocom(d):
    # {"result":{"data":[{"i":"BTC_USDT","b":"..","k":".."}]}}
    for r in d.get("result", {}).get("data", []):
        s = r.get("i", "")
        if "_" in s:
            base, _, quote = s.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("b"), r.get("k")


def _p_kraken(d):
    # {"result":{"XBTUSDT":{"a":["..."],"b":["..."]}}}
    # kraken uses XBT for BTC, ZUSD for USD — messy. Skip base rewriting;
    # cex_book will just have XBTUSDT etc — universe won't match.
    for cp, r in d.get("result", {}).items():
        if not isinstance(r, dict): continue
        base = cp
        # Kraken names: BTC/USD -> XBTUSD or XXBTZUSD. Strip Z prefix.
        for q in _QUOTES:
            if base.endswith(q):
                b = base[:-len(q)]
                if b.startswith("X") and len(b) >= 4: b = b[1:]
                a = r.get("a", [None])
                bd = r.get("b", [None])
                yield b, q, bd[0] if bd else None, a[0] if a else None
                break


def _p_p2b(d):
    # {"success":true,"result":{"BTC_USDT":{"bid":"..","ask":".."}}}
    for cp, r in (d.get("result") or {}).items():
        if "_" in cp and isinstance(r, dict):
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("bid"), r.get("ask")


def _p_exmo(d):
    # {"BTC_USDT":{"buy_price":"..","sell_price":".."}}
    for cp, r in d.items():
        if "_" in cp and isinstance(r, dict):
            base, _, quote = cp.partition("_")
            if quote in _QUOTES:
                yield base, quote, r.get("buy_price"), r.get("sell_price")


def _p_bitstamp(d):
    # [{"pair":"BTC/USDT","bid":"..","ask":".."}]
    for r in d:
        cp = r.get("pair", "")
        if "/" in cp:
            base, _, quote = cp.partition("/")
            if quote in _QUOTES:
                yield base, quote, r.get("bid"), r.get("ask")


def _p_bitfinex(d):
    # [["tBTCUSD",bid,bid_size,ask,ask_size,...]]
    for r in d:
        if not isinstance(r, list) or len(r) < 4: continue
        s = r[0]
        if isinstance(s, str) and s.startswith("t"):
            body = s[1:]
            for q in _QUOTES:
                if body.endswith(q):
                    yield body[:-len(q)], q, r[1], r[3]
                    break


ENDPOINTS: dict[str, tuple[str, Callable]] = {
    "binance":  ("https://api.binance.com/api/v3/ticker/bookTicker", _p_binance),
    "mexc":     ("https://api.mexc.com/api/v3/ticker/bookTicker", _p_mexc),
    "okx":      ("https://www.okx.com/api/v5/market/tickers?instType=SPOT", _p_okx),
    "gateio":   ("https://api.gateio.ws/api/v4/spot/tickers", _p_gate),
    "kucoin":   ("https://api.kucoin.com/api/v1/market/allTickers", _p_kucoin),
    "bybit":    ("https://api.bybit.com/v5/market/tickers?category=spot", _p_bybit),
    "bitget":   ("https://api.bitget.com/api/v2/spot/market/tickers", _p_bitget),
    "bitmart":  ("https://api-cloud.bitmart.com/spot/quotation/v3/tickers", _p_bitmart),
    "hitbtc":   ("https://api.hitbtc.com/api/3/public/ticker", _p_hitbtc),
    "whitebit": ("https://whitebit.com/api/v4/public/ticker", _p_whitebit),
    "coinex":   ("https://api.coinex.com/v2/spot/ticker", _p_coinex),
    "xt":       ("https://sapi.xt.com/v4/public/ticker/book", _p_xt),
    "lbank":    ("https://api.lbank.info/v2/supplement/ticker/bookTicker.do?symbol=all", _p_lbank),
    "bitrue":   ("https://openapi.bitrue.com/api/v1/ticker/bookTicker", _p_bitrue),
    "poloniex": ("https://api.poloniex.com/markets/ticker24h", _p_poloniex),
    "cryptocom":("https://api.crypto.com/exchange/v1/public/get-tickers", _p_cryptocom),
    "kraken":   ("https://api.kraken.com/0/public/Ticker", _p_kraken),
    "p2b":      ("https://api.p2pb2b.com/api/v2/public/tickers", _p_p2b),
    "exmo":     ("https://api.exmo.com/v1.1/ticker", _p_exmo),
    "bitstamp": ("https://www.bitstamp.net/api/v2/ticker/", _p_bitstamp),
    "bitfinex": ("https://api-pub.bitfinex.com/v2/tickers?symbols=ALL", _p_bitfinex),
}

# Exchanges that need a proxy (geo-blocked from user IP)
NEEDS_PROXY = {"binance"}


async def poll_exchange(eid: str, session: aiohttp.ClientSession,
                         book, universe_tickers: set,
                         proxies: list[str], stop_event: asyncio.Event) -> None:
    url, parser = ENDPOINTS[eid]
    fail_streak = 0
    poll_count = 0
    hits_total = 0
    started = time.monotonic()
    use_proxy = eid in NEEDS_PROXY

    while not stop_event.is_set():
        t0 = time.time()
        try:
            proxy = random.choice(proxies) if (use_proxy and proxies) else None
            async with session.get(url, headers=UA, proxy=proxy,
                                    timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
                if r.status != 200:
                    raise Exception(f"HTTP {r.status}")
                d = await r.json(content_type=None)
            poll_count += 1
            hits = 0
            for base, quote, bid, ask in parser(d):
                if base.upper() not in universe_tickers:
                    continue
                try:
                    b = float(bid) if bid is not None else None
                    a = float(ask) if ask is not None else None
                except (TypeError, ValueError):
                    continue
                if b and a:
                    await book.update(eid, f"{base}/{quote}", b, a)
                    hits += 1
            hits_total += hits
            if fail_streak > 0:
                log.info("[%s] REST poll recovered after %d fails",
                         eid, fail_streak)
            fail_streak = 0
            if poll_count % max(1, int(120 / POLL_INTERVAL)) == 0:
                el = time.monotonic() - started
                log.info("[%s] REST poll: %d cycles, %d hits, %.0fmin",
                         eid, poll_count, hits_total, el / 60)
        except asyncio.TimeoutError:
            fail_streak += 1
            if not use_proxy and fail_streak == 3:
                # 3 direct failures in a row → switch to proxy for this exchange
                log.warning("[%s] 3 direct timeouts → switching to proxy", eid)
                use_proxy = True
            if fail_streak in (1, 5, 20, 60):
                log.warning("[%s] REST poll timeout streak=%d proxy=%s",
                            eid, fail_streak, use_proxy)
        except Exception as e:
            fail_streak += 1
            if fail_streak in (1, 5, 20, 60):
                log.warning("[%s] REST poll err streak=%d: %s: %s",
                            eid, fail_streak, type(e).__name__, str(e)[:80])
        elapsed = time.time() - t0
        remaining = POLL_INTERVAL - elapsed
        if remaining > 0:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=remaining)
                return
            except asyncio.TimeoutError:
                pass


async def run_all(exchanges: dict, book, universe_tickers: set,
                   stop_event: asyncio.Event) -> None:
    proxies = load_proxies()
    # Only poll exchanges we're actually using AND have a bookTicker endpoint for
    active = {eid.partition("#")[0] for eid in exchanges}
    eids = sorted(active & set(ENDPOINTS.keys()))
    log.info("REST poller: %d exchanges (%s), %d proxies, %.1fs interval",
             len(eids), ",".join(eids), len(proxies), POLL_INTERVAL)
    conn = aiohttp.TCPConnector(limit=100, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [asyncio.create_task(
            poll_exchange(eid, session, book, universe_tickers,
                          proxies, stop_event),
            name=f"rest-poll-{eid}") for eid in eids]
        await asyncio.gather(*tasks, return_exceptions=True)
