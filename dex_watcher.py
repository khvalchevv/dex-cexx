"""DEX side — OKX OnchainOS public DEX market data (keyless). For every
CEX-sourced (chain, contract) we ask OKX for the token's candles and take the
latest close as the DEX price + 24h volUsd as an activity proxy.

Why OKX: keyless (public v5 `dex/market/candles`), aggregates price across
hundreds of DEX/CEX so coverage is ~78% of our contracts (vs ~41% DexScreener
/ ~53% GeckoTerminal), and a full sweep of ~12.8k contracts takes ~17s.

Anti-block: every request picks a RANDOM proxy from the ~1000 pool, so each IP
sees <1 req/s (≈13 requests per proxy per cycle) — far under any per-IP limit.
Browser-like headers; retries rotate to a fresh proxy on any non-200.

DexPriceBook is keyed by gid -> {contract: entry}; persisted to dex_pools.json
for a warm (instant) restart.
"""
import os
import json
import time
import random
import logging
import asyncio
import aiohttp

from exchanges import load_proxies

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
DEX_POOLS_FILE = os.path.join(_HERE, "dex_pools.json")
OKX_CANDLES = "https://web3.okx.com/api/v5/dex/market/candles"
POLL_INTERVAL_SEC = int(os.getenv("DEX_POLL_INTERVAL_SEC", "20"))
MIN_VOL_USD = float(os.getenv("DEX_MIN_VOL_USD", "1000"))   # 24h vol floor
WORKER_COUNT = int(os.getenv("DEX_WORKERS", "300"))
REQUEST_TIMEOUT = int(os.getenv("DEX_REQ_TIMEOUT", "8"))
TRACK_REFRESH_SEC = int(os.getenv("DEX_TRACK_REFRESH_SEC", "600"))
MAX_CONTRACTS_PER_GROUP = 8

OKX_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "application/json",
    "Referer": "https://web3.okx.com/",
    "Origin": "https://web3.okx.com",
}

# our normalized chain -> OKX chainIndex (EVM uses the EVM chainId).
OKX_CHAIN_INDEX = {
    "ethereum": "1", "bsc": "56", "arbitrum": "42161", "solana": "501",
    "polygon": "137", "base": "8453", "avalanche": "43114", "optimism": "10",
    "tron": "195", "sui": "784", "ton": "607", "aptos": "637", "zksync": "324",
    "linea": "59144", "scroll": "534352", "mantle": "5000", "blast": "81457",
    "sei": "1329", "cronos": "25", "fantom": "146", "gnosis": "100",
    "celo": "42220", "pulsechain": "369", "berachain": "80094", "abstract": "2741",
}

# Exchange network label -> normalized chain.
_CHAIN_ALIASES = {
    "ethereum": {"eth", "ethereum", "erc20", "erc-20", "erc 20", "weth", "ethereum mainnet"},
    "bsc": {"bsc", "bep20", "bep-20", "bep 20", "bnb", "bscbnb", "bnb smart chain",
            "bnb chain", "binance smart chain", "binance-smart-chain", "bsc_bnb"},
    "arbitrum": {"arb", "arbone", "arbitrum", "arbitrum one", "arbitrumone", "arbi",
                 "arbitrum-one", "arb one"},
    "solana": {"sol", "solana", "spl"},
    "polygon": {"matic", "polygon", "pol", "polygon pos", "matic mainnet"},
    "base": {"base"},
    "avalanche": {"avax", "avalanche", "avaxc", "avalanche c-chain", "c-chain", "avax c"},
    "optimism": {"op", "optimism", "optimistic", "op mainnet"},
    "tron": {"trx", "trc20", "trc-20", "trc 20", "tron"},
    "sui": {"sui"},
    "ton": {"ton", "the open network", "toncoin"},
    "aptos": {"apt", "aptos"},
    "zksync": {"zksync", "zksync era", "zksyncera", "zksync2", "zksyncera_eth", "era", "zksera"},
    "linea": {"linea"},
    "scroll": {"scroll"},
    "mantle": {"mantle"},
    "blast": {"blast"},
    "sei": {"sei", "sei evm", "seievm"},
    "cronos": {"cro", "cronos"},
    "fantom": {"ftm", "fantom", "sonic"},
    "gnosis": {"gnosis", "xdai"},
    "celo": {"celo"},
    "pulsechain": {"pls", "pulsechain"},
    "berachain": {"bera", "berachain"},
    "abstract": {"abstract"},
}
_NET_TO_CHAIN = {a: c for c, al in _CHAIN_ALIASES.items() for a in al}


def _norm_net(net: str) -> str | None:
    k = "".join(c for c in (net or "").lower() if c.isalnum() or c == " ").strip()
    return _NET_TO_CHAIN.get(k) or _NET_TO_CHAIN.get(k.replace(" ", ""))


def _norm_contract(c: str) -> str:
    """Canonical contract for comparison: SUI '0xPKG::mod::T' -> package addr;
    hex lowercased, leading zeros stripped, truncated to 40 (SUI 40/64 forms
    match); base58 (Solana/Tron) left as-is."""
    c = (c or "").strip()
    if not c:
        return ""
    if "::" in c:
        c = c.split("::")[0]
    if c[:2].lower() == "0x":
        body = c[2:].lstrip("0").lower()[:40]
        return "0x" + (body or "0")
    return c


class DexPriceBook:
    """gid -> {contract_lower: {chain, priceUsd, vol24h, liquidity, contract,
    dex, url, ts}}."""

    def __init__(self):
        self.book: dict[str, dict[str, dict]] = {}
        self.lock = asyncio.Lock()

    async def update(self, gid: str, contract: str, entry: dict):
        async with self.lock:
            self.book.setdefault(gid, {})[contract.lower()] = entry

    async def remove(self, gid: str, contract: str):
        """Drop a contract OKX no longer prices, so a stale price can't linger."""
        async with self.lock:
            g = self.book.get(gid)
            if g is not None:
                g.pop(contract.lower(), None)
                if not g:
                    self.book.pop(gid, None)

    async def snapshot(self) -> dict[str, dict[str, dict]]:
        async with self.lock:
            return {g: dict(items) for g, items in self.book.items()}

    async def best(self, gid: str) -> dict | None:
        async with self.lock:
            items = self.book.get(gid)
            return max(items.values(), key=lambda e: e.get("vol24h", 0)) if items else None

    def live_contracts(self) -> set:
        return {c for items in self.book.values() for c in items}

    def save(self, path: str = DEX_POOLS_FILE) -> None:
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"ts": time.time(), "book": self.book}, f)
        except Exception as e:
            log.warning("dex pools save err: %s", e)

    def load(self, path: str = DEX_POOLS_FILE) -> bool:
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return False
        except Exception as e:
            log.warning("dex pools load err: %s", e)
            return False
        self.book = data.get("book", {})
        n = sum(len(v) for v in self.book.values())
        log.info("dex pools loaded: %d pools across %d groups (age %.1fh)",
                 n, len(self.book), (time.time() - data.get("ts", 0)) / 3600)
        return True


# Sentinel: OKX definitively has no (valid) pool for this contract right now
# (code 51001, empty data, zero price, or below the volume floor). Distinct
# from None, which means a transport error (block/timeout) — there we keep any
# existing price rather than pruning it on a flaky request.
NO_POOL = object()


async def _fetch_okx(session, proxy, chain, chain_index, contract):
    """Latest price (candle close) + 24h volUsd for a contract via OKX.
    Returns an entry dict on success, NO_POOL if OKX has no valid pool, or
    None on a transport error."""
    url = (f"{OKX_CANDLES}?chainIndex={chain_index}"
           f"&tokenContractAddress={contract}&bar=1H&limit=24")
    try:
        async with session.get(url, proxy=proxy, headers=OKX_HEADERS,
                               timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as r:
            if r.status != 200:
                return None                                          # transient/block
            d = await r.json()
    except Exception:
        return None
    if d.get("code") != "0" or not d.get("data"):
        return NO_POOL                                               # OKX: no such pool
    data = d["data"]
    try:
        price = float(data[0][4])                                    # latest close
        vol = sum(float(c[6]) for c in data if len(c) > 6)           # ~24h volUsd
    except (ValueError, IndexError, TypeError):
        return NO_POOL
    if price <= 0 or vol < MIN_VOL_USD:
        return NO_POOL                                               # dead/no real activity
    return {"chain": chain, "priceUsd": price, "vol24h": vol, "liquidity": vol,
            "contract": contract, "dex": "OKX", "url": None, "ts": time.time()}


class DexWatcher:
    """Polls OKX for every CEX-sourced contract that sits on an OKX-supported
    chain, storing price+vol in DexPriceBook[gid][contract]."""

    def __init__(self, universe, book: DexPriceBook,
                  poll_interval_sec: int = POLL_INTERVAL_SEC):
        self.universe = universe
        self.book = book
        self.poll_interval = poll_interval_sec
        self.proxies = load_proxies()
        if not self.proxies:
            log.warning("no proxies — OKX calls go direct (block risk)")
        self._jobs: dict[str, tuple[str, str, str, str]] = {}  # cl -> (gid, chain, chainIndex, ticker)
        self._orig: dict[str, str] = {}
        self._last_refresh = 0.0

    def _refresh_jobs(self) -> None:
        jobs, orig = {}, {}
        skipped = 0
        for gid, g in self.universe.groups.items():
            ticker = gid.partition("#")[0]
            seen = 0
            for lst in g.get("listings", []):
                for net, nd in (lst.get("networks") or {}).items():
                    c = (nd.get("contract") or "").strip()
                    if not c or len(c) <= 6:
                        continue
                    chain = _norm_net(net)
                    ci = OKX_CHAIN_INDEX.get(chain) if chain else None
                    if not ci:
                        skipped += 1
                        continue
                    cl = c.lower()
                    if cl in jobs:
                        continue
                    jobs[cl] = (gid, chain, ci, ticker)
                    orig[cl] = c
                    seen += 1
                    if seen >= MAX_CONTRACTS_PER_GROUP:
                        break
                if seen >= MAX_CONTRACTS_PER_GROUP:
                    break
        self._jobs, self._orig = jobs, orig
        self._last_refresh = time.time()
        log.info("dex: %d contracts to poll across %d groups (%d skipped: chain n/a)",
                 len(jobs), len({v[0] for v in jobs.values()}), skipped)

    def _pick_proxy(self):
        return random.choice(self.proxies) if self.proxies else None

    async def _worker(self, queue, session, stop_event):
        while not stop_event.is_set():
            try:
                cl = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            try:
                gid, chain, ci, ticker = self._jobs[cl]
                contract = self._orig.get(cl, cl)
                # up to 2 tries, each a fresh random proxy (anti-block + flaky)
                saw_no_pool = False
                for _ in range(2):
                    e = await _fetch_okx(session, self._pick_proxy(), chain, ci, contract)
                    if e is NO_POOL:
                        saw_no_pool = True
                        continue
                    if e:
                        await self.book.update(gid, contract, e)
                        break
                else:
                    # no fresh price; if OKX said "no pool" (not just a network
                    # blip), prune the stale entry so we never serve it.
                    if saw_no_pool:
                        await self.book.remove(gid, contract)
            except Exception as ex:
                log.debug("dex worker err: %s", str(ex)[:60])
            finally:
                queue.task_done()

    async def run(self, stop_event: asyncio.Event) -> None:
        self.book.load()                       # warm start
        connector = aiohttp.TCPConnector(limit=WORKER_COUNT * 2, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            queue: asyncio.Queue = asyncio.Queue(maxsize=WORKER_COUNT * 4)
            workers = [asyncio.create_task(self._worker(queue, session, stop_event),
                                            name=f"dex-w{i}")
                       for i in range(WORKER_COUNT)]
            try:
                while not stop_event.is_set():
                    if not self._jobs or time.time() - self._last_refresh > TRACK_REFRESH_SEC:
                        self._refresh_jobs()
                    if not self._jobs:
                        await asyncio.sleep(5)
                        continue
                    keys = list(self._jobs.keys())
                    random.shuffle(keys)
                    t0 = time.time()
                    for cl in keys:
                        if stop_event.is_set():
                            break
                        await queue.put(cl)
                    await queue.join()
                    self.book.save()
                    elapsed = time.time() - t0
                    sleep_for = max(0.0, self.poll_interval - elapsed)
                    log.info("dex poll: %d contracts in %.1fs, %d live (sleep %.1fs)",
                             len(keys), elapsed, len(self.book.live_contracts()), sleep_for)
                    if sleep_for > 0:
                        await asyncio.sleep(sleep_for)
            finally:
                for w in workers:
                    w.cancel()
                await asyncio.gather(*workers, return_exceptions=True)
