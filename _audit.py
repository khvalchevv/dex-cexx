"""Per-exchange capability audit.

For every TARGET exchange, report:
  WS  : bulk watch_tickers? per-symbol watch_order_book? first message latency
  REST: currencies total / how many have networks / contracts / dep-wit flags
        (uses our currencies.py pipeline incl. bingx & lbank direct REST)
"""
import asyncio
import io
import logging
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
logging.basicConfig(level=logging.WARNING,
                     format="%(asctime)s %(levelname)s %(name)s: %(message)s")

from exchanges import (TARGET_EXCHANGES, init_exchange, init_bingx_sharded,
                         load_keys, load_proxies)
from cex_watcher import _build_symbol_list
from currencies import CurrencyCache, _norm

WS_TIMEOUT = 25.0


async def ws_probe(exch_id, inst):
    """Return (mode, first_ms) or (reason, 0)."""
    syms = _build_symbol_list(inst, exch_id)
    if not syms:
        return "no-usd-pairs", 0
    t0 = time.monotonic()
    try:
        if inst.has.get("watchTickers"):
            await asyncio.wait_for(inst.watch_tickers(syms), timeout=WS_TIMEOUT)
            return "bulk-tickers", int((time.monotonic()-t0)*1000)
        if inst.has.get("watchOrderBook"):
            await asyncio.wait_for(inst.watch_order_book(syms[0]), timeout=WS_TIMEOUT)
            return "orderbook", int((time.monotonic()-t0)*1000)
        if inst.has.get("watchTicker"):
            await asyncio.wait_for(inst.watch_ticker(syms[0]), timeout=WS_TIMEOUT)
            return "ticker", int((time.monotonic()-t0)*1000)
        return "no-ws", 0
    except asyncio.TimeoutError:
        return "WS-TIMEOUT", 0
    except Exception as e:
        return f"WS-ERR:{str(e)[:40]}", 0


async def main():
    keys = load_keys()
    proxies = load_proxies()
    cache = CurrencyCache(proxies)

    regular = [e for e in TARGET_EXCHANGES if e != "bingx"]
    results = {}

    async def audit(eid):
        row = {"ws": "-", "ws_ms": 0, "spot": 0,
               "cur_total": 0, "with_net": 0, "with_contract": 0, "with_dw": 0}
        try:
            inst = await init_exchange(eid, keys, proxies)
        except Exception as e:
            row["ws"] = f"init-EXC:{str(e)[:30]}"
            results[eid] = row
            return
        if not inst:
            row["ws"] = "init-None"
            results[eid] = row
            return
        row["spot"] = sum(1 for m in inst.markets.values() if m.get("spot"))
        # REST currencies via our pipeline
        try:
            await cache.refresh_one(eid, inst)
            entries = cache.cache.get(_norm(eid), {})
            row["cur_total"] = len(entries)
            for code, info in entries.items():
                nets = info.get("networks") or {}
                if nets:
                    row["with_net"] += 1
                if any(nd.get("contract") for nd in nets.values()):
                    row["with_contract"] += 1
                if any(nd.get("deposit") is not None or nd.get("withdraw") is not None
                       for nd in nets.values()):
                    row["with_dw"] += 1
        except Exception as e:
            row["cur_total"] = -1
        # WS probe
        mode, ms = await ws_probe(eid, inst)
        row["ws"] = mode
        row["ws_ms"] = ms
        results[eid] = row
        try:
            await inst.close()
        except Exception:
            pass

    sem = asyncio.Semaphore(6)
    async def _run(eid):
        async with sem:
            await audit(eid)
    await asyncio.gather(*[_run(e) for e in regular], return_exceptions=True)

    # bingx (sharded) — probe via one shard
    try:
        bshards = await init_bingx_sharded(keys, proxies)
        if bshards:
            eid0 = next(iter(bshards))
            inst = bshards[eid0]
            row = {"ws": "-", "ws_ms": 0,
                   "spot": sum(1 for m in inst.markets.values() if m.get("spot")),
                   "cur_total": 0, "with_net": 0, "with_contract": 0, "with_dw": 0}
            await cache.refresh_one(eid0, inst)
            entries = cache.cache.get("bingx", {})
            row["cur_total"] = len(entries)
            for code, info in entries.items():
                nets = info.get("networks") or {}
                if nets: row["with_net"] += 1
                if any(nd.get("contract") for nd in nets.values()):
                    row["with_contract"] += 1
                if any(nd.get("deposit") is not None for nd in nets.values()):
                    row["with_dw"] += 1
            syms = getattr(inst, "_shard_symbols", [])
            if syms:
                t0 = time.monotonic()
                try:
                    await asyncio.wait_for(inst.watch_ticker(syms[0]), timeout=WS_TIMEOUT)
                    row["ws"] = "per-symbol-ticker"
                    row["ws_ms"] = int((time.monotonic()-t0)*1000)
                except Exception as e:
                    row["ws"] = f"WS-ERR:{str(e)[:30]}"
            results["bingx"] = row
            for sh in bshards.values():
                try: await sh.close()
                except Exception: pass
    except Exception as e:
        results["bingx"] = {"ws": f"EXC:{str(e)[:30]}", "ws_ms": 0,
                            "spot": 0, "cur_total": 0, "with_net": 0,
                            "with_contract": 0, "with_dw": 0}

    # ---- print ----
    print()
    hdr = (f"{'exchange':<16} {'WS mode':<20} {'1st':>6} {'spot':>6} | "
           f"{'cur':>6} {'nets':>6} {'contr':>6} {'d/w':>6}")
    print(hdr)
    print("-" * len(hdr))
    for eid in sorted(results):
        r = results[eid]
        first = f"{r['ws_ms']}ms" if r['ws_ms'] else "-"
        print(f"{eid:<16} {r['ws'][:20]:<20} {first:>6} {r['spot']:>6} | "
              f"{r['cur_total']:>6} {r['with_net']:>6} {r['with_contract']:>6} {r['with_dw']:>6}")

    # ---- summaries ----
    ws_ok = [e for e,r in results.items() if r["ws"] in
             ("bulk-tickers","orderbook","ticker","per-symbol-ticker")]
    contract_ok = [e for e,r in results.items() if r["with_contract"] > 0]
    print()
    print(f"WS price feed OK ({len(ws_ok)}): {', '.join(sorted(ws_ok))}")
    print()
    print(f"REST contracts OK ({len(contract_ok)}): {', '.join(sorted(contract_ok))}")
    print()
    no_contract = [e for e in results if e not in contract_ok]
    print(f"NO contracts ({len(no_contract)}): {', '.join(sorted(no_contract))}")


if __name__ == "__main__":
    asyncio.run(main())
