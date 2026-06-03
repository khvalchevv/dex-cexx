"""Dig for project NAME in raw info / direct REST for exchanges where ccxt
name field is empty: bitget, binance, bybit, htx, coinex, lbank, whitebit."""
import asyncio, io, sys, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
import logging
logging.basicConfig(level=logging.ERROR)
import aiohttp, random
from exchanges import init_exchange, load_keys, load_proxies

TOKS = ["RON", "USDT", "PEPE", "WBTC"]


async def dump_ccxt_raw(eid, keys, proxies):
    inst = await init_exchange(eid, keys, proxies)
    if not inst:
        print(f"== {eid}: init none"); return
    inst.options["fetchCurrencies"] = True
    real = getattr(inst, "_real_fetch_currencies", None) or inst.fetch_currencies
    try:
        currs = await real()
    except Exception as e:
        print(f"== {eid}: fetch exc {str(e)[:50]}"); await inst.close(); return
    print(f"\n== {eid} ccxt raw.info keys / name search ==")
    for t in TOKS:
        info = (currs or {}).get(t)
        if not info:
            continue
        raw = info.get("info")
        # find any key with a name-ish value
        nm_top = info.get("name")
        raw_str = json.dumps(raw, default=str)[:300] if raw else "None"
        print(f"  {t}: ccxt.name={nm_top!r}")
        print(f"     raw={raw_str}")
    await inst.close()


async def direct_bitget(proxies):
    # bitget public coins endpoint
    url = "https://api.bitget.com/api/v2/spot/public/coins"
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(url, proxy=random.choice(proxies),
                              timeout=aiohttp.ClientTimeout(total=15)) as r:
                d = await r.json(content_type=None)
        except Exception as e:
            print("bitget direct exc", str(e)[:60]); return
    data = d.get("data") or []
    print(f"\n== bitget DIRECT /spot/public/coins: {len(data)} coins ==")
    for it in data[:1]:
        print("  sample keys:", list(it.keys()))
        print("  sample:", json.dumps(it, default=str)[:400])
    for t in TOKS:
        m = next((x for x in data if (x.get("coin") or "").upper() == t), None)
        if m:
            chains = m.get("chains") or []
            print(f"  {t}: coin={m.get('coin')} "
                  f"chain0={ (chains[0] if chains else {}) }"[:200])


async def main():
    keys = load_keys(); proxies = load_proxies()
    for e in ["bitget", "binance", "bybit", "htx", "coinex"]:
        try:
            await dump_ccxt_raw(e, keys, proxies)
        except Exception as ex:
            print(f"== {e}: outer {str(ex)[:50]}")
    await direct_bitget(proxies)


asyncio.run(main())
