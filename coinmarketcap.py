"""CoinMarketCap bulk contract index — supplements CoinGecko.

Uses CMC's public data-api (no key required, IP-throttled). We paginate over
their cryptocurrency listing endpoint with `aux=platform` — each page holds
100 coins with their primary (chain, contract) tuple. Rotates our proxy pool
per request so IP throttling never bites.

CMC returns ONE platform per coin (their canonical chain). CoinGecko covers
the multichain fan-out. Together they cover the widest set of contracts.
"""
import asyncio
import json
import logging
import os
import random
import time

import aiohttp

log = logging.getLogger(__name__)

HERE = os.path.dirname(os.path.abspath(__file__))
CMC_FILE = os.path.join(HERE, "cmc_coins.json")
REFRESH_SEC = int(os.getenv("CMC_REFRESH_SEC", str(24 * 3600)))
PAGE_SIZE = 100
MAX_COINS = int(os.getenv("CMC_MAX_COINS", "15000"))          # ~150 pages
CONCURRENCY = 20                                                # simultaneous pages
PAGE_TIMEOUT = 15
UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
LISTING_URL = ("https://api.coinmarketcap.com/data-api/v3/cryptocurrency/"
               "listing?start={start}&limit={limit}&sortBy=market_cap"
               "&sortType=desc&aux=platform,is_active,tags")

# CMC platform names -> our canonical chain slug. Mirrors CG_TO_OUR_CHAIN.
CMC_TO_OUR_CHAIN = {
    "Ethereum": "ethereum",
    "BNB Smart Chain (BEP20)": "bsc",
    "BNB Beacon Chain (BEP2)": "bnb",
    "Solana": "solana",
    "Polygon": "polygon",
    "Arbitrum": "arbitrum",
    "Base": "base",
    "Optimism": "optimism",
    "Avalanche C-Chain": "avalanche",
    "Tron20": "tron",
    "TRC20": "tron",
    "Tron": "tron",
    "Sui": "sui",
    "TON": "ton",
    "Aptos": "aptos",
    "zkSync Era": "zksync",
    "Linea": "linea",
    "Scroll": "scroll",
    "Mantle": "mantle",
    "Blast": "blast",
    "Sei v2": "sei",
    "Sei EVM": "sei",
    "Cronos": "cronos",
    "Fantom": "fantom",
    "Sonic": "fantom",
    "Celo": "celo",
    "Berachain": "berachain",
    "Kaia": "kaia",
    "Klaytn": "kaia",
    "Manta Pacific": "manta",
    "Merlin": "merlin",
    "Ronin": "ronin",
    "Hyperliquid EVM": "hyperevm",
    "Unichain": "unichain",
    "XRP Ledger": "xrpl",
    "Cardano": "cardano",
    "Near Protocol": "near",
    "Algorand": "algorand",
    "Cosmos": "cosmos",
    "Osmosis": "osmosis",
    "Kava": "kava",
    "Injective": "injective",
    "Bitcoin": "bitcoin",
    "Stacks": "stacks",
    "Filecoin": "filecoin",
    "IoTeX": "iotex",
    "Kadena": "kadena",
    "Kusama": "kusama",
    "Polkadot": "polkadot",
    "Waves": "waves",
    "MultiversX": "multiversx",
    "ICP": "icp",
    "Tezos": "tezos",
    "Stellar": "stellar",
    "Hedera Hashgraph": "hedera",
    "Vechain": "vechain",
    "Zilliqa": "zilliqa",
    "Neo": "neo",
    "Ontology": "ontology",
    "OKB Chain": "oktc",
    "OKT Chain": "oktc",
    "OKX Chain": "oktc",
    "Kucoin Community Chain": "kcc",
    "Ronin Chain": "ronin",
    "Taiko": "taiko",
    "Katana": "katana",
    "Monad": "monad",
    "Worldchain": "worldchain",
    "Wormhole": "solana",       # wormhole-wrapped assets on solana
}


def _load_proxies() -> list[str]:
    try:
        from exchanges import load_proxies
        return load_proxies()
    except Exception:
        return []


async def _fetch_page(session, start: int, proxies: list[str]) -> list[dict]:
    """Fetch one page with proxy rotation until success or exhausted."""
    url = LISTING_URL.format(start=start, limit=PAGE_SIZE)
    tries = [None]                            # direct
    tries += [random.choice(proxies) if proxies else None for _ in range(3)]
    for proxy in tries:
        try:
            async with session.get(url, headers=UA, proxy=proxy,
                                    timeout=aiohttp.ClientTimeout(total=PAGE_TIMEOUT)) as r:
                if r.status != 200:
                    continue
                d = await r.json(content_type=None)
                items = (d.get("data") or {}).get("cryptoCurrencyList") or []
                if items:
                    return items
        except Exception:
            continue
    return []


async def fetch_all_coins(proxies: list[str] | None = None) -> list[dict]:
    """Paginated bulk fetch. Returns the deduplicated raw item list."""
    if proxies is None:
        proxies = _load_proxies()
    starts = list(range(1, MAX_COINS + 1, PAGE_SIZE))
    log.info("CMC: fetching %d pages × %d, %d proxies",
             len(starts), PAGE_SIZE, len(proxies))
    sem = asyncio.Semaphore(CONCURRENCY)

    async def _guarded(session, s):
        async with sem:
            return await _fetch_page(session, s, proxies)

    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)
    t0 = time.time()
    async with aiohttp.ClientSession(connector=conn) as s:
        results = await asyncio.gather(*[_guarded(s, st) for st in starts])
    flat: dict[int, dict] = {}
    for page in results:
        for it in page:
            if isinstance(it, dict) and it.get("id"):
                flat[it["id"]] = it
    log.info("CMC: fetched %d unique coins in %.1fs",
             len(flat), time.time() - t0)
    return list(flat.values())


def build_contract_index(coins: list[dict]) -> dict[tuple[str, str], int]:
    """Reverse index (chain_our_slug, contract_lower) -> cmc_id."""
    out: dict[tuple[str, str], int] = {}
    for c in coins:
        cid = c.get("id")
        if not cid:
            continue
        p = c.get("platform")
        if not p:
            continue
        cmc_name = (p.get("name") or "").strip()
        addr = (p.get("tokenAddress") or p.get("token_address") or "").strip().lower()
        our_chain = CMC_TO_OUR_CHAIN.get(cmc_name)
        if not our_chain or len(addr) < 7:
            continue
        out[(our_chain, addr)] = cid
    return out


def save(coins: list[dict], path: str = CMC_FILE) -> None:
    payload = {"ts": time.time(), "coins": coins}
    tmp = path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f)
        os.replace(tmp, path)
    except Exception as e:
        log.warning("CMC save err: %s", e)


def load(path: str = CMC_FILE) -> tuple[list[dict], float]:
    try:
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        return d.get("coins", []), d.get("ts", 0)
    except FileNotFoundError:
        return [], 0
    except Exception as e:
        log.warning("CMC load err: %s", e)
        return [], 0


async def refresh_loop(stop_event: asyncio.Event) -> None:
    """Fetch on startup if stale, then every REFRESH_SEC."""
    coins, ts = load()
    age = time.time() - ts
    if not coins or age > REFRESH_SEC:
        log.info("CMC index: refreshing (age %.1fh, %d cached)",
                 age / 3600, len(coins))
        try:
            fresh = await fetch_all_coins()
            if fresh:
                save(fresh)
        except Exception as e:
            log.warning("CMC initial fetch err: %s", e)
    else:
        log.info("CMC index: cached %d coins (age %.1fh) — fresh enough",
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
        except Exception as e:
            log.warning("CMC refresh err: %s", e)
