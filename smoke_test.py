"""Smoke test for all TARGET_EXCHANGES:
  1) load_markets()  — does the exchange respond at all?
  2) watch_tickers() — does the WS deliver at least one ticker within WS_TIMEOUT?

Run:  python smoke_test.py
"""
import asyncio
import io
import logging
import sys
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

logging.basicConfig(level=logging.WARNING,
                     format="%(asctime)s %(levelname)s %(name)s: %(message)s",
                     datefmt="%H:%M:%S")

from exchanges import (TARGET_EXCHANGES, init_exchange, init_bingx_sharded,
                         load_keys, load_proxies)
from cex_watcher import _build_symbol_list

WS_TIMEOUT = 30.0  # seconds to wait for first ticker batch


async def _one_ws_tick(exch_id: str, inst) -> tuple[str, int, float] | None:
    """Wait for first WS message. Returns (mode, pairs_or_levels, elapsed_sec) or None.
    mode is one of: 'tickers' (bulk), 'orderbook' (per-symbol), or None."""
    symbols = _build_symbol_list(inst, exch_id)
    if not symbols:
        return None
    t0 = time.monotonic()
    if inst.has.get("watchTickers"):
        tickers = await inst.watch_tickers(symbols)
        return "tickers", len(tickers), time.monotonic() - t0
    if inst.has.get("watchOrderBook"):
        ob = await inst.watch_order_book(symbols[0])
        bids = ob.get("bids") or []
        return "orderbook", len(bids), time.monotonic() - t0
    if inst.has.get("watchTicker"):
        t = await inst.watch_ticker(symbols[0])
        return "ticker", (1 if t.get("bid") and t.get("ask") else 0), time.monotonic() - t0
    return None


async def test_one(exch_id: str, keys, proxies) -> dict:
    res = {"id": exch_id, "load": "?", "ws": "?", "mode": "-",
           "ws_ms": 0, "pairs": 0, "err": ""}
    inst = None
    try:
        inst = await init_exchange(exch_id, keys, proxies)
        if not inst:
            res["load"] = "FAIL"
            res["err"] = "init_exchange returned None"
            return res
        pairs = sum(1 for m in inst.markets.values() if m.get("spot"))
        res["load"] = "OK"
        res["pairs"] = pairs

        ws_result = await asyncio.wait_for(_one_ws_tick(exch_id, inst), timeout=WS_TIMEOUT)
        if ws_result is None:
            res["ws"] = "NO_WS"
        else:
            mode, cnt, elapsed = ws_result
            res["mode"] = mode
            res["ws"] = "OK" if cnt > 0 else "EMPTY"
            res["ws_ms"] = int(elapsed * 1000)
    except asyncio.TimeoutError:
        res["ws"] = "TIMEOUT"
        res["err"] = f">{WS_TIMEOUT}s"
    except Exception as e:
        msg = str(e)[:140]
        if res["load"] == "?":
            res["load"] = "FAIL"
        elif res["ws"] == "?":
            res["ws"] = "FAIL"
        res["err"] = msg
    finally:
        if inst is not None:
            try:
                await inst.close()
            except Exception:
                pass
    return res


async def main():
    keys = load_keys()
    proxies = load_proxies()
    regular = [e for e in TARGET_EXCHANGES if e != "bingx"]
    print(f"testing {len(TARGET_EXCHANGES)} exchanges (timeout {WS_TIMEOUT}s per WS) ...")
    print(f"proxies loaded: {len(proxies)}, keys: {sum(1 for k in keys.values() if isinstance(k, dict) and k.get('apiKey'))}")
    print()

    results = await asyncio.gather(*[test_one(e, keys, proxies) for e in regular])

    if "bingx" in TARGET_EXCHANGES:
        # bingx is sharded — light test: init one probe, one watch_ticker on first symbol
        try:
            probe = await init_exchange("bingx", keys, proxies)
            if probe:
                syms = [s for s, m in probe.markets.items()
                         if m.get("spot") and m.get("active")
                         and m.get("quote") in ("USDT", "USDC", "USD")]
                t0 = time.monotonic()
                try:
                    await asyncio.wait_for(probe.watch_ticker(syms[0]), timeout=WS_TIMEOUT)
                    results.append({"id": "bingx", "load": "OK", "ws": "OK",
                                    "mode": "ticker",
                                    "ws_ms": int((time.monotonic() - t0) * 1000),
                                    "pairs": len(syms), "err": ""})
                except asyncio.TimeoutError:
                    results.append({"id": "bingx", "load": "OK", "ws": "TIMEOUT",
                                    "mode": "ticker", "ws_ms": 0, "pairs": len(syms),
                                    "err": f">{WS_TIMEOUT}s"})
                await probe.close()
            else:
                results.append({"id": "bingx", "load": "FAIL", "ws": "-",
                                "mode": "-", "ws_ms": 0, "pairs": 0,
                                "err": "init failed"})
        except Exception as e:
            results.append({"id": "bingx", "load": "FAIL", "ws": "-",
                            "mode": "-", "ws_ms": 0, "pairs": 0,
                            "err": str(e)[:120]})

    # ---- print table ----
    print(f"{'exchange':<18} {'load':<6} {'ws':<8} {'mode':<10} {'pairs':>6} {'first_tick':>11}  notes")
    print("-" * 100)
    ok_ws, ok_load_only, failed = 0, 0, 0
    for r in sorted(results, key=lambda x: x["id"]):
        first = f"{r['ws_ms']}ms" if r['ws_ms'] else "-"
        notes = r["err"] if r["err"] else ""
        print(f"{r['id']:<18} {r['load']:<6} {r['ws']:<8} {r.get('mode','-'):<10} "
              f"{r['pairs']:>6} {first:>11}  {notes[:50]}")
        if r["load"] == "OK" and r["ws"] == "OK":
            ok_ws += 1
        elif r["load"] == "OK":
            ok_load_only += 1
        else:
            failed += 1
    print("-" * 100)
    print(f"summary: ws_ok={ok_ws}  load_only={ok_load_only}  failed={failed}  total={len(results)}")


if __name__ == "__main__":
    asyncio.run(main())
