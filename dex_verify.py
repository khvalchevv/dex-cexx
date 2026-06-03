"""DEX alert verification via DexScreener (keyless).

OKX (dex_watcher) is the broad, fast price/coverage source — it sweeps ~12.8k
contracts in seconds. But its aggregator also returns prices for pools that are
NOT real markets:

  * PHANTOM — OKX reports a price + (wash-traded) volume, but the contract has
    no real on-chain pool. DexScreener, which indexes the actual DEX pools,
    returns ZERO pairs (Fractal Bitcoin: OKX $2.49/$58k vol, real liquidity
    ~$100; ROGIN.AI, The9bit, TRUMP-on-tron: same).
  * WRONG TOKEN — the CEX-reported contract is a different project sharing the
    ticker (Meteora's ETH contract is actually Metronome "MET").
  * DEAD — a real but abandoned pool, tiny liquidity (old Theta ERC20 ~$4k).

So before emitting a CEX<->DEX alert we VERIFY the DEX leg against DexScreener:
the contract must have a real pool, with liquidity above a floor, whose token
symbol/name matches the group. This needs no price ratios — it's an identity +
real-market check. Only alert *candidates* are verified (a few dozen, cached),
so the cost is tiny.
"""
import os
import re
import time
import random
import logging
import asyncio
import aiohttp

from exchanges import load_proxies

log = logging.getLogger(__name__)

DEXSCREENER_TOKENS = "https://api.dexscreener.com/latest/dex/tokens/"
MIN_LIQ_USD = float(os.getenv("DEX_MIN_LIQ_USD", "10000"))   # real-market floor
MIN_POOL_VOL_USD = float(os.getenv("DEX_MIN_POOL_VOL", "5000"))  # real 24h trading
VERIFY_TTL = int(os.getenv("DEX_VERIFY_TTL", "120"))          # cache verdict (s)
VERIFY_TIMEOUT = int(os.getenv("DEX_VERIFY_TIMEOUT", "8"))

_UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Accept": "application/json",
}

# our normalized chain -> DexScreener chainId slug.
_DS_CHAIN = {
    "ethereum": "ethereum", "bsc": "bsc", "solana": "solana", "polygon": "polygon",
    "arbitrum": "arbitrum", "base": "base", "avalanche": "avalanche",
    "optimism": "optimism", "tron": "tron", "sui": "sui", "ton": "ton",
    "aptos": "aptos", "zksync": "zksync", "linea": "linea", "scroll": "scroll",
    "mantle": "mantle", "blast": "blast", "sei": "sei", "cronos": "cronos",
    "fantom": "fantom", "gnosis": "gnosis", "celo": "celo", "pulsechain": "pulsechain",
    "berachain": "berachain", "abstract": "abstract",
}


def _norm_word(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _words(s: str) -> set:
    return {w for w in re.split(r"[^a-z0-9]+", (s or "").lower()) if len(w) >= 3}


def _identity_ok(ticker: str, display: str, ds_symbol: str, ds_name: str) -> bool:
    """True if the DexScreener token plausibly IS the group's token.

    symbol must relate to the ticker; if it does but the *name* clearly names a
    different project (Meteora vs 'Metronome2'), reject."""
    tk = _norm_word(ticker).lstrip("$")
    sym = _norm_word(ds_symbol).lstrip("$")
    if not sym:
        return False
    sym_ok = (sym == tk) or (len(tk) >= 3 and tk in sym) or (len(sym) >= 3 and sym in tk)
    if not sym_ok:
        return False
    # symbol matches — guard against same-ticker different-project via the name.
    dwords, nwords = _words(display), _words(ds_name)
    if dwords and nwords and not (dwords & nwords):
        # display name and DexScreener name share no significant word -> suspect.
        # only reject when both names are real (non-empty) and disjoint.
        return False
    return True


class DexVerifier:
    """Verifies a (chain, contract) DEX leg against DexScreener. Caches verdicts
    for VERIFY_TTL so the 5s scan loop doesn't re-query the same candidates."""

    def __init__(self):
        self.proxies = load_proxies()
        self._cache: dict[str, tuple[float, dict | None]] = {}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(limit=50, ssl=False))
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def verify(self, chain: str, contract: str, ticker: str,
                     display: str) -> dict | None:
        """Return {priceUsd, liquidity, symbol, name, chain} if the contract has
        a real, liquid pool whose token matches the group; else None.
        Result is cached per (chain, contract)."""
        if not chain or not contract:
            return None
        key = f"{chain}:{contract.lower()}:{(ticker or '').upper()}"
        hit = self._cache.get(key)
        now = time.time()
        if hit and now - hit[0] < VERIFY_TTL:
            return hit[1]
        result = await self._query(chain, contract, ticker, display)
        self._cache[key] = (now, result)
        return result

    async def _query(self, chain, contract, ticker, display) -> dict | None:
        session = await self._get_session()
        url = DEXSCREENER_TOKENS + contract
        ds_chain = _DS_CHAIN.get(chain)
        data = None
        for _ in range(3):
            proxy = random.choice(self.proxies) if self.proxies else None
            try:
                async with session.get(url, proxy=proxy, headers=_UA,
                                       timeout=aiohttp.ClientTimeout(total=VERIFY_TIMEOUT)) as r:
                    if r.status != 200:
                        continue
                    data = await r.json()
                    break
            except Exception:
                continue
        if data is None:
            return None                          # couldn't reach -> treat as unverified
        pairs = data.get("pairs") or []
        # Keep pairs on this chain that are a REAL market: enough liquidity AND
        # real 24h trading. A broken/manipulated pool (Ancient8's $5.5k token
        # price) has liquidity dumped in but ~no volume — the volume floor drops
        # it. Among the survivors pick the most-traded (highest h24 volume) — its
        # price is the real one, not the highest-liquidity broken pool's.
        def _liq(p):
            return (p.get("liquidity") or {}).get("usd") or 0.0
        def _vol(p):
            return (p.get("volume") or {}).get("h24") or 0.0
        cand = [p for p in pairs
                if (not ds_chain or p.get("chainId") == ds_chain)
                and _liq(p) >= MIN_LIQ_USD and _vol(p) >= MIN_POOL_VOL_USD]
        if not cand:
            return None              # PHANTOM / DEAD / broken: no real, traded pool
        best = max(cand, key=_vol)
        bt = best.get("baseToken") or {}
        if not _identity_ok(ticker, display, bt.get("symbol", ""), bt.get("name", "")):
            return None                          # WRONG TOKEN: ticker/name mismatch
        try:
            price = float(best.get("priceUsd"))
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None
        return {"priceUsd": price, "liquidity": _liq(best), "vol24h": _vol(best),
                "symbol": bt.get("symbol"), "name": bt.get("name"), "chain": chain}
