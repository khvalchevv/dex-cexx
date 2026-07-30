"""CoinGecko bulk canonical index of tokens × chains × contracts.

One call `/coins/list?include_platform=true` returns every listed token with
its per-chain contract mapping. Cached as cg_coins.json and refreshed every
CG_REFRESH_SEC (24 h by default).

Used to AUGMENT the CEX-derived universe: many CEXes report a token on 1-2
chains only, while the token actually lives on 5+ (Wormhole/LayerZero/Stargate
wraps, native deployments, etc). CG's platforms field is the closest thing to
a canonical multichain contract registry we can get keyless.
"""
import asyncio
import json
import logging
import os
import random
import time

import aiohttp

from exchanges import load_proxies

log = logging.getLogger(__name__)

CG_LIST_URL = "https://api.coingecko.com/api/v3/coins/list"
CG_API_KEY = os.getenv("CG_API_KEY", "").strip()
REFRESH_SEC = int(os.getenv("CG_REFRESH_SEC", str(24 * 3600)))       # 24 h
TIMEOUT = int(os.getenv("CG_TIMEOUT", "60"))
RETRIES = 4

HERE = os.path.dirname(os.path.abspath(__file__))
CG_FILE = os.path.join(HERE, "cg_coins.json")

# CoinGecko platform slug -> our canonical chain (matches dex_watcher._norm_net).
# Anything not here we drop silently (usually cosmos/appchains we don't cover).
CG_TO_OUR_CHAIN = {
    "ethereum":              "ethereum",
    "binance-smart-chain":   "bsc",
    "polygon-pos":           "polygon",
    "arbitrum-one":          "arbitrum",
    "arbitrum-nova":         "arbitrum",
    "optimistic-ethereum":   "optimism",
    "base":                  "base",
    "avalanche":             "avalanche",
    "fantom":                "fantom",
    "solana":                "solana",
    "tron":                  "tron",
    "linea":                 "linea",
    "scroll":                "scroll",
    "blast":                 "blast",
    "berachain":             "berachain",
    "cronos":                "cronos",
    "sui":                   "sui",
    "the-open-network":      "ton",
    "ton":                   "ton",
    "aptos":                 "aptos",
    "zksync":                "zksync",
    "mantle":                "mantle",
    "sei-v2":                "sei",
    "sei-network":           "sei",
    "xdai":                  "gnosis",
    "gnosis":                "gnosis",
    "celo":                  "celo",
    "pulsechain":            "pulsechain",
    "abstract":              "abstract",
    "klay-token":            "klaytn",
    "kaia":                  "klaytn",
    "ronin":                 "ronin",
    "kucoin-community-chain": "kcc",
    "kcc-community-chain":   "kcc",
    "merlin-chain":          "merlin",
    "okexchain":             "oktc",
    "xdc-network":           "xdc",
    "hyperliquid":           "hyperevm",
    "world-chain":           "worldchain",
    "unichain":              "unichain",
    "sonic":                 "fantom",                              # Sonic = Fantom rebrand
    "robinhood":             "robinhood",
    "robinhood-chain":       "robinhood",
}


async def fetch_all_coins(proxies: list[str] | None = None) -> list[dict]:
    """One-shot bulk fetch of every CoinGecko coin with its platforms map."""
    if proxies is None:
        proxies = load_proxies()
    params = {"include_platform": "true"}
    headers = {"Accept": "application/json"}
    if CG_API_KEY:
        headers["x-cg-demo-api-key"] = CG_API_KEY
    conn = aiohttp.TCPConnector(limit=4, ssl=False)
    async with aiohttp.ClientSession(connector=conn) as s:
        for attempt in range(RETRIES):
            proxy = random.choice(proxies) if proxies else None
            try:
                async with s.get(CG_LIST_URL, params=params, headers=headers,
                                 proxy=proxy,
                                 timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
                    if r.status == 200:
                        return await r.json()
                    log.warning("CG list HTTP %d (attempt %d)", r.status, attempt + 1)
            except Exception as e:
                log.warning("CG list err (attempt %d): %s", attempt + 1, e)
    return []


def build_index(coins: list[dict]) -> dict[str, list[dict]]:
    """Group CoinGecko coins by upper-case symbol so we can look them up quickly.

    Returns { "USDC": [ {"id","name","platforms":{our_slug: address}}, ...], ...}.
    Multiple entries per symbol are common (real + copycats + wrapped variants);
    the caller resolves by contract collision when there's ambiguity.
    """
    idx: dict[str, list[dict]] = {}
    for c in coins:
        sym = (c.get("symbol") or "").upper().strip()
        if not sym:
            continue
        plats_raw = c.get("platforms") or {}
        # normalize CG slugs -> our chain slugs; drop unknown platforms; ignore
        # empty-key entries (native tokens have "": "" pair which is meaningless
        # for contract lookup).
        our_plats: dict[str, str] = {}
        for cg_slug, addr in plats_raw.items():
            if not cg_slug or not addr:
                continue
            our_chain = CG_TO_OUR_CHAIN.get(cg_slug)
            if not our_chain:
                continue
            addr_l = addr.strip().lower()
            if len(addr_l) > 6:
                our_plats[our_chain] = addr_l
        idx.setdefault(sym, []).append({
            "id": c.get("id", ""),
            "name": (c.get("name") or "").strip(),
            "platforms": our_plats,
        })
    return idx


def build_contract_index(coins: list[dict]) -> dict[tuple[str, str], str]:
    """Reverse index for cross-chain contract → canonical CG coin id.

    Returns { (chain_our_slug, contract_lower): cg_id }. Used by the universe
    builder to merge exchange listings whose contracts live on different
    chains but belong to the same project (WETH on eth vs base, USDC on eth
    vs sol vs polygon, etc.)."""
    out: dict[tuple[str, str], str] = {}
    for c in coins:
        cg_id = (c.get("id") or "").strip()
        if not cg_id:
            continue
        for cg_slug, addr in (c.get("platforms") or {}).items():
            if not cg_slug or not addr:
                continue
            our_chain = CG_TO_OUR_CHAIN.get(cg_slug)
            if not our_chain:
                continue
            a = addr.strip().lower()
            if len(a) > 6:
                out[(our_chain, a)] = cg_id
    return out


def save(coins: list[dict], path: str = CG_FILE) -> None:
    payload = {"ts": time.time(), "coins": coins}
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("CG save err: %s", e)


def load(path: str = CG_FILE) -> tuple[list[dict], float]:
    """Returns (coins_list, ts) or ([], 0) if missing/corrupt."""
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d.get("coins", []), d.get("ts", 0)
    except FileNotFoundError:
        return [], 0
    except Exception as e:
        log.warning("CG load err: %s", e)
        return [], 0


async def refresh_loop(stop_event: asyncio.Event) -> None:
    """Fetch on startup if stale, then every REFRESH_SEC."""
    coins, ts = load()
    age = time.time() - ts
    if not coins or age > REFRESH_SEC:
        log.info("CG index: refreshing (age %.1fh, %d cached)", age / 3600, len(coins))
        fresh = await fetch_all_coins()
        if fresh:
            save(fresh)
            log.info("CG index: fetched %d coins", len(fresh))
    else:
        log.info("CG index: cached %d coins (age %.1fh) — fresh enough",
                 len(coins), age / 3600)
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=REFRESH_SEC)
            return
        except asyncio.TimeoutError:
            pass
        try:
            fresh = await fetch_all_coins()
            if fresh:
                save(fresh)
                log.info("CG index: refreshed to %d coins", len(fresh))
        except Exception as e:
            log.exception("CG refresh err: %s", e)
