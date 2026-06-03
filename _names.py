"""Check which exchanges expose a real project NAME (not just echo of symbol)
in their currencies data, plus what RON / wRON look like per exchange."""
import asyncio, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import logging
logging.basicConfig(level=logging.ERROR)

from exchanges import TARGET_EXCHANGES, init_exchange, load_keys, load_proxies

PROBE_TOKENS = ["RON", "WRON", "USDT", "PEPE", "STETH", "WBTC", "ETH"]


async def one(eid, keys, proxies, out):
    try:
        inst = await init_exchange(eid, keys, proxies)
    except Exception as e:
        out[eid] = f"init-exc {str(e)[:30]}"; return
    if not inst:
        out[eid] = "init-none"; return
    inst.options["fetchCurrencies"] = True
    real = getattr(inst, "_real_fetch_currencies", None) or inst.fetch_currencies
    try:
        currs = await real()
    except Exception as e:
        out[eid] = f"fetch-exc {str(e)[:30]}"
        await inst.close(); return
    currs = currs or {}
    # how many have a name != symbol
    named = 0
    samples = {}
    for code, info in currs.items():
        if not info: continue
        nm = (info.get("name") or "").strip()
        if nm and nm.upper() != code.upper():
            named += 1
        if code.upper() in PROBE_TOKENS:
            samples[code.upper()] = nm or "(empty)"
    has_name = "YES" if named > len(currs) * 0.3 else ("SOME" if named else "NO")
    out[eid] = {"total": len(currs), "named": named, "name_field": has_name,
                "samples": samples}
    await inst.close()


async def main():
    keys = load_keys(); proxies = load_proxies()
    out = {}
    sem = asyncio.Semaphore(6)
    async def run(e):
        async with sem:
            try: await one(e, keys, proxies, out)
            except Exception as ex: out[e] = f"outer {str(ex)[:30]}"
    await asyncio.gather(*[run(e) for e in TARGET_EXCHANGES if e != "bingx"])

    print(f"\n{'exchange':<16} {'name?':<6} {'total':>6} {'named':>6}  RON / WRON / STETH samples")
    print("-"*90)
    for e in sorted(out):
        r = out[e]
        if isinstance(r, str):
            print(f"{e:<16} {r}")
            continue
        s = r["samples"]
        smp = " | ".join(f"{k}={v[:18]}" for k, v in s.items()) or "—"
        print(f"{e:<16} {r['name_field']:<6} {r['total']:>6} {r['named']:>6}  {smp}")


asyncio.run(main())
