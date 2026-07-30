"""Continuous DexScreener pool refresh — batch endpoint + proxy rotation.

DexScreener's `/latest/dex/tokens/{addresses}` supports comma-separated
lists up to 30 contract addresses per request. Combined with our 500-proxy
rotation this shrinks a full universe cycle to ~10-15s (vs the old 30-min
polling window).

Layout:
  self.book: gid -> {chain:contract: {priceUsd,liquidity,vol24h,url,ts}}

Data flow: build (gid, contract) job list from universe → chunk into
30-address batches → fire concurrent GETs on rotating proxies → update
`self.book` incrementally (per-batch), so scanner sees fresh prices mid-cycle.
"""
import asyncio
import json
import logging
import os
import random
import time
from collections import defaultdict

import aiohttp

from dex_watcher import _norm_contract, OKX_CHAIN_INDEX
from exchanges import load_proxies

log = logging.getLogger(__name__)

DS_URL = "https://api.dexscreener.com/latest/dex/tokens/"
BATCH_SIZE = 30                                                   # DS max per call
CONCURRENCY = int(os.getenv("DS_CONCURRENCY", "200"))
TIMEOUT = int(os.getenv("DS_TIMEOUT", "8"))
IDLE_SEC = float(os.getenv("DS_IDLE_SEC", "5"))                   # pause between full cycles
RETRIES = 3
_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

HERE = os.path.dirname(os.path.abspath(__file__))
DS_POOLS_FILE = os.path.join(HERE, "ds_pools.json")


class DexScreenerBook:
    """Mirror of DexPriceBook. Keyed by gid -> {f"{chain}:{contract}": entry}."""

    def __init__(self):
        self.book: dict[str, dict[str, dict]] = {}
        self.ts: float = 0.0
        self.lock = asyncio.Lock()

    async def snapshot(self) -> dict:
        async with self.lock:
            return {gid: dict(d) for gid, d in self.book.items()}

    def save(self) -> None:
        try:
            tmp = DS_POOLS_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"ts": self.ts, "book": self.book}, f)
            os.replace(tmp, DS_POOLS_FILE)
        except Exception as e:
            log.warning("ds book save err: %s", e)

    def load(self) -> bool:
        try:
            with open(DS_POOLS_FILE, encoding="utf-8") as f:
                d = json.load(f)
        except FileNotFoundError:
            return False
        except Exception as e:
            log.warning("ds book load err: %s", e)
            return False
        self.book = d.get("book", {})
        self.ts = d.get("ts", 0)
        age_h = (time.time() - self.ts) / 3600
        live = sum(len(v) for v in self.book.values())
        log.info("ds book loaded: %d gids, %d pools (age %.1fh)",
                 len(self.book), live, age_h)
        return True


def _pick_best_pool_per_chain(pairs, requested_contract):
    """Given DS `pairs` for one token, return {chain: best_entry} — highest
    h24-vol pool per chain. Includes OKX-covered chains too: the scanner
    prefers OKX prices when both sources have the same (gid, chain, contract),
    so DS naturally acts as fallback where OKX misses a pool."""
    best_per_chain: dict[str, dict] = {}
    req_c = requested_contract.lower()
    for p in pairs or []:
        ch = p.get("chainId")
        if not ch:
            continue
        try:
            price = float(p.get("priceUsd") or 0)
        except (TypeError, ValueError):
            price = 0.0
        if price <= 0:
            continue
        try:
            vol = float((p.get("volume") or {}).get("h24") or 0)
        except (TypeError, ValueError):
            vol = 0.0
        cur = best_per_chain.get(ch)
        if cur is not None and vol <= cur["vol24h"]:
            continue
        try:
            liq = float((p.get("liquidity") or {}).get("usd") or 0)
        except (TypeError, ValueError):
            liq = 0.0
        bt = p.get("baseToken") or {}
        best_per_chain[ch] = {
            "chain": ch,
            "priceUsd": price,
            "vol24h": vol,
            "liquidity": liq,
            "contract": requested_contract,
            "dex": p.get("dexId") or "dexscreener",
            "symbol": bt.get("symbol"),
            "name": bt.get("name"),
            "url": p.get("url"),
            "ts": time.time(),
        }
    return best_per_chain


class DexScreenerWatcher:
    """Continuous DS refresh — batched, proxy-rotated, incremental writes."""

    def __init__(self, universe, book: DexScreenerBook):
        self.universe = universe
        self.book = book
        self.proxies = load_proxies()
        self._sem = asyncio.Semaphore(CONCURRENCY)

    async def run(self, stop_event: asyncio.Event) -> None:
        self.book.load()
        log.info("ds watcher: %d proxies, batch=%d, concurrency=%d",
                 len(self.proxies), BATCH_SIZE, CONCURRENCY)
        while not stop_event.is_set():
            try:
                await self._refresh(stop_event)
            except Exception as e:
                log.exception("ds refresh err: %s", e)
            if stop_event.is_set():
                return
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=IDLE_SEC)
                return
            except asyncio.TimeoutError:
                pass

    async def _fetch_batch(self, session, batch: list[str]) -> dict[str, list] | None:
        """Fetch a comma-joined batch of up to 30 contracts. Returns
        {contract_lower: pairs_list}. Rotates proxy on each retry."""
        url = DS_URL + ",".join(batch)
        for _ in range(RETRIES):
            proxy = random.choice(self.proxies) if self.proxies else None
            try:
                async with session.get(url, proxy=proxy, headers=_UA,
                                        timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
                    if r.status == 429:
                        await asyncio.sleep(0.2)
                        continue
                    if r.status != 200:
                        continue
                    d = await r.json()
            except Exception:
                continue
            # DS returns { pairs: [...] } where each pair has baseToken.address
            pairs = d.get("pairs") or []
            out: dict[str, list] = defaultdict(list)
            wanted = {c.lower() for c in batch}
            for p in pairs:
                addr = ((p.get("baseToken") or {}).get("address") or "").lower()
                if addr in wanted:
                    out[addr].append(p)
            return out
        return None

    async def _refresh(self, stop_event: asyncio.Event) -> None:
        # Build (gid, contract) job list — unique per contract
        jobs: list[tuple[str, str]] = []
        seen: set[str] = set()
        for gid, g in self.universe.groups.items():
            for lst in g.get("listings", []):
                for _, nd in (lst.get("networks") or {}).items():
                    c = _norm_contract(nd.get("contract") or "")
                    if not c or len(c) < 7:
                        continue
                    if c.lower() in seen:
                        continue
                    seen.add(c.lower())
                    jobs.append((gid, c))
        if not jobs:
            return

        started = time.time()
        # Chunk into 30-address batches; each batch keeps a gid map so we know
        # which group each returned contract belongs to.
        batches: list[list[tuple[str, str]]] = [
            jobs[i:i + BATCH_SIZE] for i in range(0, len(jobs), BATCH_SIZE)
        ]
        stats = {"batches_done": 0, "hits": 0, "empty": 0, "fail": 0}

        conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)
        async with aiohttp.ClientSession(connector=conn) as session:
            async def worker(batch_jobs: list[tuple[str, str]]):
                if stop_event.is_set():
                    return
                addrs = [c for _, c in batch_jobs]
                async with self._sem:
                    result = await self._fetch_batch(session, addrs)
                stats["batches_done"] += 1
                if result is None:
                    stats["fail"] += 1
                    return
                # Group per-contract entries by gid
                per_gid: dict[str, dict[str, dict]] = defaultdict(dict)
                for gid, contract in batch_jobs:
                    pairs = result.get(contract.lower()) or []
                    if not pairs:
                        stats["empty"] += 1
                        continue
                    best = _pick_best_pool_per_chain(pairs, contract)
                    for ch, entry in best.items():
                        per_gid[gid][f"{ch}:{contract.lower()}"] = entry
                        stats["hits"] += 1
                if per_gid:
                    async with self.book.lock:
                        for gid, entries in per_gid.items():
                            self.book.book.setdefault(gid, {}).update(entries)

            await asyncio.gather(*[worker(b) for b in batches])

        async with self.book.lock:
            self.book.ts = time.time()
        self.book.save()
        el = time.time() - started
        pool_total = sum(len(v) for v in self.book.book.values())
        log.info("ds cycle: %d batches (%d addrs), %d pools total, %d hits, "
                 "%d empty, %d fail in %.1fs (%.0f addr/s)",
                 stats["batches_done"], len(jobs), pool_total,
                 stats["hits"], stats["empty"], stats["fail"],
                 el, len(jobs) / max(el, 0.001))
