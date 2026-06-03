"""Manual verification CLI.

Usage:
    python check_token.py SYMBOL

For the given token symbol, prints:
  1. CoinGecko candidates: every (chain, contract, cg_id) it knows for that ticker.
  2. Per-exchange data: contract + deposit/withdraw + per-network "match" verdict.

Match verdict per (exchange, network):
  ✓  contract resolves to one of the CG cg_ids for the input symbol -> verified same token
  ✗  contract resolves to a DIFFERENT cg_id -> ticker collision, would be filtered
  ?  no contract returned by exchange (most KRW/EU markets, phemex top-level only)
  -  network code couldn't be mapped to a DexScreener chain slug
"""
import asyncio
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from exchanges import init_all, close_all, load_proxies
from currencies import CurrencyCache, _norm
from token_resolver import TokenResolver
from alerter import cex_net_to_dex_chain


def _v(deposit: bool, withdraw: bool) -> str:
    return ("Y" if deposit else "N") + ("Y" if withdraw else "N")


async def main():
    if len(sys.argv) < 2:
        print("usage: python check_token.py SYMBOL")
        return
    token = sys.argv[1].upper()
    print(f"=== {token} ===\n")

    resolver = TokenResolver()
    await resolver.load()
    cands = resolver.resolve(token)
    cg_ids_for_token = set(c[2] for c in cands)
    print(f"CoinGecko candidates for '{token}': {len(cands)}")
    if cands:
        unique_cgs = sorted(cg_ids_for_token)
        print(f"  cg_ids: {unique_cgs[:10]}{' +...' if len(unique_cgs)>10 else ''}")
        for chain, addr, cg_id in cands[:15]:
            print(f"  {cg_id:30}  {chain:12}  {addr}")
    print()

    print("Initializing CEXes (load_markets, ~20-30s)...")
    exchanges = await init_all()
    cache = CurrencyCache(load_proxies())
    seen, unique = set(), []
    for eid, inst in exchanges.items():
        nid = _norm(eid)
        if nid in seen:
            continue
        seen.add(nid)
        unique.append((eid, inst))
    print(f"Refreshing currencies for {len(unique)} exchanges in parallel...\n")
    await asyncio.gather(
        *[cache.refresh_one(eid, inst) for eid, inst in unique],
        return_exceptions=True,
    )

    print(f"--- Per-exchange data for '{token}' ---\n")
    header = (f"{'exchange':<18} {'network':<18} {'D/W':<4} "
              f"{'contract':<48} {'cg_id':<28} match")
    print(header)
    print("-" * len(header))
    matched_legs = 0
    collision_legs = 0
    missing_legs = 0
    no_data_exchanges = []
    for eid in sorted(cache.cache.keys()):
        entries = cache.cache[eid]
        info = entries.get(token)
        if not info:
            no_data_exchanges.append(eid)
            continue
        nets = info.get("networks") or {}
        if not nets:
            print(f"{eid:<18} (no networks)")
            continue
        for n, nd in sorted(nets.items()):
            contract = nd.get("contract") or ""
            dex_chain = cex_net_to_dex_chain(n)
            cg_id = (resolver.cg_id_for_contract(dex_chain, contract)
                     if (dex_chain and contract) else None)
            if cg_id and cg_id in cg_ids_for_token:
                match = "OK"; matched_legs += 1
            elif cg_id:
                match = "COLLISION"; collision_legs += 1
            elif contract and not dex_chain:
                match = "-"
            else:
                match = "?"; missing_legs += 1
            print(f"{eid:<18} {n[:18]:<18} {_v(bool(nd.get('deposit')), bool(nd.get('withdraw'))):<4} "
                  f"{contract[:48]:<48} {(cg_id or ''):<28} {match}")
    print()
    print(f"Summary for '{token}':")
    print(f"  verified network legs: {matched_legs}")
    print(f"  collision network legs: {collision_legs}")
    print(f"  missing-contract legs (would be 'unverified'): {missing_legs}")
    print(f"  exchanges without any '{token}' currency entry: {len(no_data_exchanges)}")
    if no_data_exchanges:
        print(f"     {', '.join(no_data_exchanges)}")
    await close_all(exchanges)


if __name__ == "__main__":
    asyncio.run(main())
