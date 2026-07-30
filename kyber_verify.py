"""KyberSwap verifier — second independent confirmation of a DEX alert candidate.

OKX (dex_watcher) provides the broad price + coverage, DexScreener
(dex_verify) verifies pool existence + liquidity + symbol identity, and this
verifier asks KyberSwap's aggregator for an *executable* USDC→token swap
quote. If Kyber can route the swap with non-trivial USD output, the token is
genuinely tradeable RIGHT NOW. Keyless. Used in PARALLEL with the DexScreener
verifier — a leg passes if EITHER returns a non-None result, since the two
sources cover different DEXes and one alone misses some real tokens.

Restricted to the EVM chains KyberSwap aggregates. For non-EVM chains
(solana/tron/ton/sui/aptos/...) the verifier returns None and the leg falls
back entirely to DexScreener.
"""
import asyncio
import logging
import os
import random
import time

import aiohttp

from exchanges import load_proxies

log = logging.getLogger(__name__)

ROUTES_URL = "https://aggregator-api.kyberswap.com/{slug}/api/v1/routes"
VERIFY_TTL = int(os.getenv("KYBER_VERIFY_TTL", "180"))
TIMEOUT = int(os.getenv("KYBER_VERIFY_TIMEOUT", "8"))
SWAP_USD = float(os.getenv("KYBER_SWAP_USD", "1000"))         # nominal swap size
MIN_OUT_USD = float(os.getenv("KYBER_MIN_OUT_USD", "100"))    # require ≥$100 out of $1000 in
                                                              # (any worse = broken/illiquid)

_UA = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

# our normalized chain -> (Kyber URL chain slug, reference USDC contract, USDC decimals).
# Used as the source token for the routes quote. Chains not in this map are skipped.
_CHAINS = {
    "ethereum":      ("ethereum",      "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", 6),
    "bsc":           ("bsc",           "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", 18),
    "polygon":       ("polygon",       "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359", 6),
    "arbitrum":      ("arbitrum",      "0xaf88d065e77c8cc2239327c5edb3a432268e5831", 6),
    "optimism":      ("optimism",      "0x0b2c639c533813f4aa9d7837caf62653d097ff85", 6),
    "base":          ("base",          "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", 6),
    "avalanche":     ("avalanche",     "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e", 6),
    "fantom":        ("fantom",        "0x29219dd400f2bf60e5a23d13be72b486d4038894", 6),
    "linea":         ("linea",         "0x176211869ca2b568f2a7d4ee941e073a821ee1ff", 6),
    "scroll":        ("scroll",        "0x06efdbff2a14a7c8e15944d1f4a48f9f95f663a4", 6),
    "zksync":        ("zksync",        "0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4", 6),
    "mantle":        ("mantle",        "0x09bc4e0d864854c6afb6eb9a9cdf58ac190d0df9", 6),
    "cronos":        ("cronos",        "0xc21223249ca28397b4b6541dffaecc539bff0c59", 6),
    # Blast uses native USDB as the dollar reference (no USDC there).
    "blast":         ("blast",         "0x4300000000000000000000000000000000000003", 18),
}


class KyberVerifier:
    """Executable-quote verifier via KyberSwap aggregator (~420 DEXes, 17 chains)."""

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

    def supports_chain(self, chain: str) -> bool:
        """True iff Kyber's aggregator covers this chain — scanner skips us on
        non-EVM (solana/tron/ton/...) and relies on DexScreener alone there."""
        return chain in _CHAINS

    async def verify(self, chain: str, contract: str,
                     ticker: str = "", display: str = "") -> dict | None:
        """Returns {amountOutUsd, source} if Kyber can swap $1000 USDC into
        the token with ≥$100 output value; None if no route / unsupported /
        flat-broken. Cached for VERIFY_TTL seconds."""
        if not chain or not contract:
            return None
        cfg = _CHAINS.get(chain)
        if not cfg:
            return None                                            # chain not on Kyber
        slug, ref_addr, ref_dec = cfg
        c_lower = contract.lower()
        if c_lower == ref_addr.lower():
            return {"amountOutUsd": SWAP_USD, "source": "kyber_self_ref"}
        key = f"{chain}:{c_lower}"
        now = time.time()
        hit = self._cache.get(key)
        if hit and now - hit[0] < VERIFY_TTL:
            return hit[1]
        result = await self._query(slug, ref_addr, ref_dec, contract)
        self._cache[key] = (now, result)
        return result

    async def _query(self, slug, ref_addr, ref_dec, contract):
        session = await self._get_session()
        amount_in = int(SWAP_USD * 10 ** ref_dec)
        url = ROUTES_URL.format(slug=slug)
        params = {"tokenIn": ref_addr, "tokenOut": contract,
                  "amountIn": str(amount_in)}
        for _ in range(3):
            proxy = random.choice(self.proxies) if self.proxies else None
            try:
                async with session.get(url, params=params, proxy=proxy,
                                       headers=_UA,
                                       timeout=aiohttp.ClientTimeout(total=TIMEOUT)) as r:
                    if r.status != 200:
                        continue
                    d = await r.json()
                    if d.get("code") != 0:
                        return None                                # explicit "no route"
                    rs = (d.get("data") or {}).get("routeSummary") or {}
                    try:
                        out_usd = float(rs.get("amountOutUsd") or 0)
                    except (TypeError, ValueError):
                        return None
                    if out_usd < MIN_OUT_USD:
                        return None                                # broken / illiquid
                    return {
                        "amountOutUsd": out_usd,
                        "amountInUsd": float(rs.get("amountInUsd") or SWAP_USD),
                        "source": "kyber",
                    }
            except Exception:
                continue
        return None
