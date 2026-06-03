"""Resolve CEX symbol (e.g. "BMT") -> list of (chain, contract_address) across chains.

Source: CoinGecko `/coins/list?include_platform=true` returns every coin with a
`platforms` map of {coingecko_chain_id: contract_address}. We cache the full
list locally for 24h (it's ~12k entries, ~3-5 MB JSON) and resolve by symbol.

Symbol collisions (multiple coins share the same ticker) are common — we keep
all candidates; the DEX watcher will rank by DexScreener liquidity at poll time.
"""
import os
import json
import time
import logging
import asyncio
import aiohttp

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
CACHE_FILE = os.path.join(_HERE, "token_cache.json")
CACHE_TTL_SEC = 24 * 3600

CG_BASE = "https://api.coingecko.com/api/v3"
CG_PRO_BASE = "https://pro-api.coingecko.com/api/v3"

# CoinGecko platform_id -> DexScreener chain slug
# DexScreener chain slugs: https://docs.dexscreener.com/api/reference
CG_TO_DEX_CHAIN = {
    "ethereum": "ethereum",
    "binance-smart-chain": "bsc",
    "solana": "solana",
    "polygon-pos": "polygon",
    "arbitrum-one": "arbitrum",
    "optimistic-ethereum": "optimism",
    "base": "base",
    "avalanche": "avalanche",
    "fantom": "fantom",
    "tron": "tron",
    "cronos": "cronos",
    "linea": "linea",
    "scroll": "scroll",
    "zksync": "zksync",
    "blast": "blast",
    "mantle": "mantle",
    "celo": "celo",
    "moonbeam": "moonbeam",
    "moonriver": "moonriver",
    "kava": "kava",
    "metis-andromeda": "metis",
    "harmony-shard-0": "harmony",
    "aurora": "aurora",
    "sui": "sui",
    "aptos": "aptos",
    "ton": "ton",
    "near-protocol": "near",
    "hyperliquid": "hyperliquid",
}


def _cg_headers() -> dict:
    key = os.getenv("COINGECKO_API_KEY", "").strip()
    return {"x-cg-pro-api-key": key} if key else {}


def _cg_base() -> str:
    return CG_PRO_BASE if os.getenv("COINGECKO_API_KEY", "").strip() else CG_BASE


async def _fetch_coin_list(session: aiohttp.ClientSession) -> list[dict]:
    url = f"{_cg_base()}/coins/list"
    async with session.get(url, params={"include_platform": "true"},
                           headers=_cg_headers(), timeout=aiohttp.ClientTimeout(total=30)) as r:
        r.raise_for_status()
        return await r.json()


def _load_cache() -> dict | None:
    try:
        with open(CACHE_FILE, encoding="utf-8") as f:
            data = json.load(f)
        if time.time() - data.get("ts", 0) < CACHE_TTL_SEC:
            return data
    except FileNotFoundError:
        return None
    except Exception as e:
        log.warning("cache read err: %s", e)
    return None


def _save_cache(by_symbol: dict, coin_count: int) -> None:
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({"ts": time.time(), "coins": coin_count,
                       "by_symbol": by_symbol}, f)
    except Exception as e:
        log.warning("cache write err: %s", e)


def _build_index(coins: list[dict]) -> dict[str, list[dict]]:
    """symbol (uppercase) -> [{id, name, platforms: {chain: addr}}]"""
    by_symbol: dict[str, list[dict]] = {}
    for c in coins:
        sym = (c.get("symbol") or "").upper().strip()
        if not sym:
            continue
        platforms = {k: v for k, v in (c.get("platforms") or {}).items() if v}
        if not platforms:
            continue  # native-only coins (e.g. BTC, ETH) — handled separately
        by_symbol.setdefault(sym, []).append({
            "id": c.get("id"),
            "name": c.get("name"),
            "platforms": platforms,
        })
    return by_symbol


class TokenResolver:
    """Resolves symbol -> list of (dex_chain, contract_address, cg_id) candidates,
    plus a reverse map (chain, contract_lower) -> cg_id for collision detection."""

    def __init__(self):
        self._by_symbol: dict[str, list[dict]] = {}
        self._by_contract: dict[tuple[str, str], str] = {}  # (dex_chain, addr_lower) -> cg_id
        self._loaded = False

    async def load(self) -> None:
        cached = _load_cache()
        if cached:
            self._by_symbol = cached["by_symbol"]
            log.info("token cache hit: %d symbols (age %.1fh)",
                     len(self._by_symbol),
                     (time.time() - cached["ts"]) / 3600)
            self._build_contract_index()
            self._loaded = True
            return
        log.info("fetching CoinGecko /coins/list (this takes ~10-20s)...")
        async with aiohttp.ClientSession() as s:
            coins = await _fetch_coin_list(s)
        log.info("got %d coins from CG, building symbol index...", len(coins))
        self._by_symbol = _build_index(coins)
        _save_cache(self._by_symbol, len(coins))
        log.info("token cache saved: %d symbols", len(self._by_symbol))
        self._build_contract_index()
        self._loaded = True

    def _build_contract_index(self) -> None:
        for sym, coins in self._by_symbol.items():
            for c in coins:
                cg_id = c.get("id")
                if not cg_id:
                    continue
                for cg_chain, addr in (c.get("platforms") or {}).items():
                    dex_chain = CG_TO_DEX_CHAIN.get(cg_chain)
                    if not dex_chain or not addr:
                        continue
                    self._by_contract[(dex_chain, addr.lower())] = cg_id
        log.info("contract index built: %d (chain, address) entries",
                 len(self._by_contract))

    def resolve(self, symbol: str) -> list[tuple[str, str, str]]:
        if not self._loaded:
            return []
        out: list[tuple[str, str, str]] = []
        for c in self._by_symbol.get(symbol.upper(), []):
            for cg_chain, addr in c["platforms"].items():
                dex_chain = CG_TO_DEX_CHAIN.get(cg_chain)
                if not dex_chain or not addr:
                    continue
                out.append((dex_chain, addr, c["id"]))
        return out

    def cg_id_for_contract(self, dex_chain: str, contract: str) -> str | None:
        if not contract:
            return None
        return self._by_contract.get((dex_chain.lower(), contract.lower()))


async def _selftest():
    logging.basicConfig(level=logging.INFO)
    r = TokenResolver()
    await r.load()
    for sym in ("PEPE", "BMT", "MOCA", "USDC", "BTC"):
        candidates = r.resolve(sym)
        print(f"{sym:8} -> {len(candidates)} candidates")
        for chain, addr, cg_id in candidates[:3]:
            print(f"    {chain:12} {addr}  ({cg_id})")


if __name__ == "__main__":
    asyncio.run(_selftest())
