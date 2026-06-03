"""ccxt.pro CEX init — ported from ws-arb."""
import os
import json
import logging
import random
import asyncio
import ccxt.pro as ccxtpro

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
KEYS_FILE = os.path.join(_HERE, "api_keys.json")
PROXIES_FILE = os.path.join(_HERE, "proxies.txt")

TARGET_EXCHANGES = [
    # native bulk watchTickers
    "binance", "bybit", "okx", "kucoin", "mexc", "bitget", "gateio",
    "bingx", "kraken", "bitmart",
    "coinbaseadvanced", "cryptocom", "phemex", "whitebit",
    "coinex", "xt", "exmo", "poloniex",
    "bitvavo", "hitbtc",
    "upbit", "bithumb", "p2b",
    # WS only via per-symbol watchOrderBook (top-of-book extracted)
    "htx", "bitfinex", "lbank", "hashkey",
    "bitstamp", "bitrue", "ascendex",
]
# Excluded: probit (403), coinone (KRW-only).
# Not in ccxt.pro 4.4.50 — need custom WS client later:
#   digifinex, bigone, latoken, orangex

EXCH_WITH_KEYS = {"binance", "bybit", "mexc", "okx", "htx", "coinex",
                  "bingx", "coinbase"}


def load_keys() -> dict:
    try:
        with open(KEYS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def load_proxies() -> list[str]:
    out = []
    try:
        with open(PROXIES_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("http"):
                    out.append(line)
                else:
                    parts = line.split(":")
                    if len(parts) == 4:
                        ip, port, user, pwd = parts
                        out.append(f"http://{user}:{pwd}@{ip}:{port}")
                    elif len(parts) == 2:
                        out.append(f"http://{line}")
    except FileNotFoundError:
        pass
    return out


_KEY_MAP = {
    "binance": "binance_keys", "bybit": "bybit_keys", "mexc": "mexc_keys",
    "okx": "okx_keys", "htx": "htx_keys", "coinex": "coinex_keys",
    "bingx": "bingx_keys", "coinbaseadvanced": "coinbase_keys",
}


def _build_inst(ccxt_id: str, cfg: dict, proxies: list[str]) -> ccxtpro.Exchange:
    inst = getattr(ccxtpro, ccxt_id)(cfg)
    if ccxt_id in ("bybit", "gateio", "bingx", "lbank", "phemex", "htx"):
        inst.options["defaultType"] = "spot"
    # okx & bitget expect fetchMarkets as a list of type strings.
    if ccxt_id in ("okx", "bitget"):
        inst.options["fetchMarkets"] = ["spot"]
    # mexc & htx expect a dict with a "types" sub-key.
    if ccxt_id in ("mexc", "htx"):
        inst.options["fetchMarkets"] = {
            "types": {"spot": True, "linear": False, "inverse": False}
        }
    # Windows clock skew workaround
    if ccxt_id in ("bybit", "mexc", "binance"):
        inst.options["recvWindow"] = 60000
    # Coinbase Advanced tries to fetch fee tiers from a private endpoint during
    # load_markets when keys are present — opt out.
    if ccxt_id == "coinbaseadvanced":
        inst.options["fetchMarketsFromAllAccounts"] = False
        inst.options["fetchFees"] = False
    if proxies and ccxt_id not in EXCH_WITH_KEYS:
        inst.aiohttp_proxy = random.choice(proxies)
    return inst


async def _skip_currencies(*a, **kw):
    """Monkey-patch: prevent ccxt's load_markets from auto-invoking
    fetch_currencies on instances where the private endpoint is rate-limited
    (notably bingx — researcher uses this trick)."""
    return {}


async def init_exchange(ccxt_id: str, keys: dict, proxies: list[str]) -> ccxtpro.Exchange | None:
    if not hasattr(ccxtpro, ccxt_id):
        log.warning("ccxt.pro does not support %s", ccxt_id)
        return None
    base_cfg = {"enableRateLimit": True, "timeout": 25000,
                "options": {"fetchCurrencies": False}}
    k = keys.get(_KEY_MAP.get(ccxt_id, ""), {})

    # Researcher pattern: build instance WITHOUT keys in cfg so ccxt doesn't
    # decide to hit private endpoints during load_markets, then attach keys
    # manually afterwards. Also monkey-patch fetch_currencies to no-op — our
    # currencies.py uses direct REST for the venues that need auth.
    inst = _build_inst(ccxt_id, dict(base_cfg), proxies)
    # Stash the real fetch_currencies so currencies.py can still call it on demand.
    inst._real_fetch_currencies = inst.fetch_currencies  # type: ignore[attr-defined]
    inst.fetch_currencies = _skip_currencies  # type: ignore[method-assign]
    if k.get("apiKey"):
        inst.apiKey = k["apiKey"]
        inst.secret = k["secret"]
        if "password" in k:
            inst.password = k["password"]
    try:
        await inst.load_markets()
    except Exception as e:
        err = str(e)[:120]
        log.warning("FAIL %-20s (with keys): %s", ccxt_id, err)
        try:
            await inst.close()
        except Exception:
            pass
        # Retry without keys — for market data we don't need auth.
        if k.get("apiKey"):
            log.info("retry %-20s without keys", ccxt_id)
            inst = _build_inst(ccxt_id, dict(base_cfg), proxies)
            inst._real_fetch_currencies = inst.fetch_currencies  # type: ignore[attr-defined]
            inst.fetch_currencies = _skip_currencies  # type: ignore[method-assign]
            try:
                await inst.load_markets()
            except Exception as e2:
                log.warning("FAIL %-20s (no keys): %s", ccxt_id, str(e2)[:120])
                try:
                    await inst.close()
                except Exception:
                    pass
                return None
            # Re-attach keys post-load so currencies.py direct REST still has them.
            inst.apiKey = k["apiKey"]
            inst.secret = k["secret"]
            if "password" in k:
                inst.password = k["password"]
        else:
            return None
    spot_count = sum(1 for m in inst.markets.values() if m.get("spot"))
    log.info("OK %-20s loaded (%d spot pairs)", ccxt_id, spot_count)
    return inst


BINGX_SHARD_SIZE = 150
_USD_QUOTES = ("USDT", "USDC", "USD", "BUSD", "DAI", "FDUSD")


async def init_bingx_sharded(keys: dict, proxies: list[str]) -> dict[str, ccxtpro.Exchange]:
    """BingX spot has no bulk WS — shard into N instances x ~150 symbols each."""
    probe = await init_exchange("bingx", keys, proxies)
    if not probe:
        return {}
    symbols = [s for s, m in (probe.markets or {}).items()
               if m.get("spot") and m.get("active")
               and m.get("quote") in _USD_QUOTES]
    await probe.close()
    if not symbols:
        log.warning("bingx: 0 spot/USD pairs")
        return {}
    chunks = [symbols[i:i + BINGX_SHARD_SIZE]
              for i in range(0, len(symbols), BINGX_SHARD_SIZE)]
    log.info("bingx: %d spot pairs -> %d shard(s) of ~%d",
             len(symbols), len(chunks), BINGX_SHARD_SIZE)

    async def _make_shard(idx: int, chunk: list[str]):
        inst = await init_exchange("bingx", keys, proxies)
        if inst:
            inst._shard_symbols = chunk
        return idx, inst

    results = await asyncio.gather(*[_make_shard(i, c) for i, c in enumerate(chunks)])
    out = {}
    for idx, inst in results:
        if inst:
            out[f"bingx#{idx}"] = inst
    log.info("bingx: %d/%d shards ready", len(out), len(chunks))
    return out


async def init_all() -> dict[str, ccxtpro.Exchange]:
    keys = load_keys()
    proxies = load_proxies()
    regular = [e for e in TARGET_EXCHANGES if e != "bingx"]
    log.info("init: %d exchanges (bingx sharded), %d proxies",
             len(TARGET_EXCHANGES), len(proxies))
    reg_task = asyncio.gather(
        *[init_exchange(eid, keys, proxies) for eid in regular],
        return_exceptions=True,
    )
    bingx_task = init_bingx_sharded(keys, proxies) if "bingx" in TARGET_EXCHANGES else None
    reg_results, bingx_shards = await asyncio.gather(
        reg_task, bingx_task if bingx_task else asyncio.sleep(0, result={}),
    )
    out = {}
    for eid, inst in zip(regular, reg_results):
        if isinstance(inst, ccxtpro.Exchange):
            out[eid] = inst
    out.update(bingx_shards)
    log.info("ready: %d active instances", len(out))
    return out


async def close_all(exchanges: dict[str, ccxtpro.Exchange]) -> None:
    await asyncio.gather(
        *[e.close() for e in exchanges.values()],
        return_exceptions=True,
    )
