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
    "bingx", "kraken",
    "coinbaseadvanced", "cryptocom", "phemex", "whitebit",
    "coinex", "xt",
    # WS only via per-symbol watchOrderBook (top-of-book extracted)
    "lbank", "bitrue",
]
# Excluded (user-disabled):
#   poloniex, bitmart, exmo, bitstamp, hashkey, bitfinex, hitbtc, htx, ascendex,
#   bitvavo (geo-block), bithumb (KRW-only), upbit (KRW-only), p2b (bad endpoint),
#   probit (403), coinone (KRW-only).
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


def _build_inst(ccxt_id: str, cfg: dict, proxies: list[str],
                 use_proxy: bool = True) -> ccxtpro.Exchange:
    inst = getattr(ccxtpro, ccxt_id)(cfg)
    if ccxt_id in ("bybit", "gateio", "bingx", "lbank", "phemex", "htx"):
        inst.options["defaultType"] = "spot"
    # okx & bitget expect fetchMarkets as a list of type strings.
    if ccxt_id in ("okx", "bitget"):
        inst.options["fetchMarkets"] = ["spot"]
    # okx ccxt bug: parse_market crashes on some instruments where baseCcy=None.
    # Wrap the class-level parse_market to skip malformed entries.
    if ccxt_id == "okx":
        _orig_parse_market = inst.parse_market
        def _safe_parse_market(m, *a, **kw):
            if not (m.get("baseCcy") and m.get("quoteCcy")):
                return None
            return _orig_parse_market(m, *a, **kw)
        inst.parse_market = _safe_parse_market  # type: ignore[method-assign]
        # And filter None out of parse_markets result
        _orig_parse_markets = inst.parse_markets
        def _safe_parse_markets(mkts, *a, **kw):
            filtered = [m for m in (mkts or [])
                        if m.get("baseCcy") and m.get("quoteCcy")]
            return _orig_parse_markets(filtered, *a, **kw)
        inst.parse_markets = _safe_parse_markets  # type: ignore[method-assign]
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
    if use_proxy and proxies and ccxt_id not in EXCH_WITH_KEYS:
        inst.aiohttp_proxy = random.choice(proxies)
    return inst


async def _skip_currencies(*a, **kw):
    """Monkey-patch: prevent ccxt's load_markets from auto-invoking
    fetch_currencies on instances where the private endpoint is rate-limited
    (notably bingx — researcher uses this trick)."""
    return {}


INIT_MAX_ATTEMPTS = 4


async def init_exchange(ccxt_id: str, keys: dict, proxies: list[str]) -> ccxtpro.Exchange | None:
    """Load-markets an exchange with retries: try up to INIT_MAX_ATTEMPTS times
    with a FRESH proxy each time. Most failures were proxy timeouts, not real
    exchange problems — one dead proxy shouldn't nuke the whole exchange for
    the rest of the session."""
    if not hasattr(ccxtpro, ccxt_id):
        log.warning("ccxt.pro does not support %s", ccxt_id)
        return None
    base_cfg = {"enableRateLimit": True, "timeout": 25000,
                "options": {"fetchCurrencies": False}}
    k = keys.get(_KEY_MAP.get(ccxt_id, ""), {})

    async def _try(with_keys: bool, use_proxy: bool):
        inst = _build_inst(ccxt_id, dict(base_cfg), proxies, use_proxy=use_proxy)
        inst._real_fetch_currencies = inst.fetch_currencies  # type: ignore[attr-defined]
        inst.fetch_currencies = _skip_currencies  # type: ignore[method-assign]
        if with_keys and k.get("apiKey"):
            inst.apiKey = k["apiKey"]
            inst.secret = k["secret"]
            if "password" in k:
                inst.password = k["password"]
        try:
            await inst.load_markets()
            return inst, None
        except Exception as e:
            try: await inst.close()
            except Exception: pass
            return None, str(e)[:120]

    # Attempt sequence: direct → proxy retries. Direct is fastest & most
    # reliable when the exchange isn't geo-blocked from us; proxies are the
    # backup when a public endpoint refuses our residential IP.
    last_err = ""
    attempts = [(True, False)]                         # direct with keys
    attempts += [(True, True)] * (INIT_MAX_ATTEMPTS - 1)  # proxied
    for i, (with_keys, use_proxy) in enumerate(attempts, 1):
        inst, err = await _try(with_keys=with_keys, use_proxy=use_proxy)
        if inst is not None:
            if i > 1:
                log.info("OK %-20s loaded on attempt %d (proxy=%s)",
                         ccxt_id, i, use_proxy)
            break
        last_err = err
        if k.get("apiKey") and ("Unauthorized" in err or "401" in err
                                 or "invalid" in err.lower()):
            log.info("retry %-20s without keys (auth err)", ccxt_id)
            inst, err = await _try(with_keys=False, use_proxy=use_proxy)
            if inst is not None:
                inst.apiKey = k["apiKey"]; inst.secret = k["secret"]
                if "password" in k: inst.password = k["password"]
                break
            last_err = err
    else:
        log.warning("FAIL %-20s after %d attempts: %s",
                    ccxt_id, len(attempts), last_err)
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
