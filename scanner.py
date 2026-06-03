"""Scanner — CEX-CEX only, universe-driven.

The token universe (built in universe.py) groups every exchange listing of
the same economic asset together — across mismatched tickers (RONIN / RON /
WRON) and wrapper variants. Name-grouping is precise for that, but a single
project name sometimes spans *different* tradeable assets (World Liberty
Financial = WLFI governance + USD1 stablecoin; "Neiro" memecoin forks; the
"BITCOIN" memecoin sharing Bitcoin's name). Comparing those produces fake
mega-spreads.

So the scanner segments each group by *price density* before computing a
spread — NOT a median, NOT a %-cap, NOT CoinGecko (all rejected by design):

  * members at one price level form a price cluster;
  * the dominant cluster (most exchanges) is the asset's true price;
  * a tight intra-cluster spread is a normal same-asset arb;
  * one/two outlier venues vs the dominant cluster, within a sane price
    window, is a real single-venue dislocation (the Coinbase-4x case) — we
    still alert it;
  * two comparably-sized clusters far apart are two different sub-assets
    (WLFI vs USD1, BTC vs WBT) — scanned independently, never cross-compared;
  * an absurd price ratio (memecoin "BITCOIN" vs BTC, ~1e6x) is never a
    cross-compare candidate.

No CoinGecko, no median, no DEX (this phase).
"""
import asyncio
import logging
import os
import time

from cex_watcher import CexPriceBook
from dex_watcher import _norm_net, _norm_contract

log = logging.getLogger(__name__)

SPREAD_BLACKLIST = {"ascendex", "hitbtc", "bitfinex"}  # prices historically unreliable
MAX_ALERTS_PER_TICK = int(os.getenv("MAX_ALERTS_PER_TICK", "5"))

# Price-density segmentation knobs (env-overridable).
# Split a group into clusters where consecutive sorted mid-prices jump by
# more than CLUSTER_GAP x. RON/RONIN/WRON sit at ~1.0x (stay together); a 4x
# venue dislocation forms its own singleton cluster.
CLUSTER_GAP = float(os.getenv("CLUSTER_GAP", "1.6"))
# A non-dominant cluster is treated as an arbitrage *outlier* (compared
# against the dominant cluster) only if it spans at most this many distinct
# exchanges. Bigger -> it's a distinct sub-asset, scanned on its own.
# Kept at 1 (true singleton venue, e.g. Coinbase-4x case) because during WS
# warmup partial price coverage makes legit groups look like 1-2 ex clusters
# transiently — anything looser leaks fake mega-spreads.
OUTLIER_MAX_EX = int(os.getenv("OUTLIER_MAX_EX", "1"))
# Never cross-compare two clusters whose price ratio is outside this window.
# 8x preserves Coinbase-4x dislocation (ratio 4) and the legit ~6-7x outliers
# while killing the 15-20x fake spreads observed on over-merged groups
# (WLFI/USD1 ratio ~5, Selfkey ticker collision ratio ~15).
CROSS_RATIO_MAX = float(os.getenv("CROSS_RATIO_MAX", "8"))
# CEX<->DEX: ignore a DEX price that diverges from the CEX price by more than
# this factor. The same token essentially never holds a >4x gap (it'd be
# arbed away instantly); a larger gap means the contract field points at a
# different/bridged/abandoned DEX pool, not the real market (THETA 8.8x,
# MEME 8.2x were exactly this). 4x still leaves room for genuine new-listing
# dislocations up to ~300%.
DEX_SANITY_RATIO = float(os.getenv("DEX_SANITY_RATIO", "4"))
# DEX prices older than this are ignored (stale). On a warm start the pool
# file may be hours old; the refresh loop re-prices within ~20s, so until then
# those entries are skipped rather than compared at a stale price.
DEX_MAX_AGE_SEC = int(os.getenv("DEX_MAX_AGE_SEC", "300"))
# A group is one token (contract-grouped), so every venue should price near
# the same level. A DEX price diverging from the dominant CEX price by more
# than this factor is a wrong/dead contract the CEX mis-reported (e.g. OKX
# returning ~$0 for a token's bad ETH contract) — drop that DEX leg. This is a
# token-identity guard, NOT an arb cap: real arbs (<15x) all still alert.
DEX_MAX_RATIO = float(os.getenv("DEX_MAX_RATIO", "15"))


def _mid(q: dict) -> float:
    return (q["bid"] + q["ask"]) / 2.0


def _clusterize(quotes: list[dict]) -> list[list[dict]]:
    """Split quotes into price clusters along big sorted-mid-price gaps."""
    qs = sorted(quotes, key=_mid)
    clusters: list[list[dict]] = [[qs[0]]]
    for q in qs[1:]:
        prev = _mid(clusters[-1][-1])
        if prev > 0 and _mid(q) / prev > CLUSTER_GAP:
            clusters.append([q])
        else:
            clusters[-1].append(q)
    return clusters


async def scan_once(cex_book: CexPriceBook, universe, min_spread_pct: float,
                     max_age_sec: int, blacklist=None, dex_snap=None,
                     verifier=None) -> list[dict]:
    """One pass over the universe. Returns a list of alert dicts.
    dex_snap: optional {gid: {chain: dex_entry}} for CEX<->DEX comparison."""
    snap = await cex_book.snapshot()  # {SYMBOL: {eid: {pair,bid,ask,ts}}}
    now = time.time()
    alerts: list[dict] = []

    for gid, group in universe.groups.items():
        # Same ticker can split into gid + "gid#1" + ... when listings disagree
        # on the project name. Blacklisting the base ticker must cover them all,
        # so check both the full gid and the "#N"-stripped base.
        base_gid = gid.partition("#")[0]
        if blacklist is not None and (blacklist.contains_sync(gid)
                                      or blacklist.contains_sync(base_gid)):
            continue

        # Collect every fresh quote across all member listings of this group.
        quotes: list[dict] = []
        for lst in group["listings"]:
            eid = lst["eid"]
            if eid in SPREAD_BLACKLIST:
                continue
            sym = lst["symbol"].upper()
            ex_map = snap.get(sym)
            if not ex_map:
                continue
            q = ex_map.get(eid)
            if not q or now - q["ts"] >= max_age_sec:
                continue
            if not q.get("bid") or not q.get("ask") or q["ask"] <= 0:
                continue
            quotes.append({
                "eid": eid, "symbol": sym, "pair": q["pair"],
                "bid": q["bid"], "ask": q["ask"],
                "wrapped": lst.get("wrapped", False),
                "wrap_kind": lst.get("wrap_kind"),
            })

        if len(quotes) < 1:
            continue

        # CEX side: cluster to isolate the dominant price level (cross-asset
        # safety — keeps WLFI out of a USD1 comparison if a name-split missed).
        cexv = quotes
        if quotes:
            clusters = _clusterize(quotes)
            cexv = max(clusters, key=lambda c: len({q["eid"] for q in c}))

        # DEX venues: pools of this group (one token), fresh, and within a sane
        # price ratio of the dominant CEX level. The ratio guard drops a DEX
        # leg whose price is a wrong/dead contract the CEX mis-reported (OKX
        # gives no symbol, so a bad ETH contract returns ~$0 -> huge fake).
        dom_ref = sum(_mid(q) for q in cexv) / len(cexv) if cexv else 0.0
        dexv = []
        if dex_snap is not None:
            for e in (dex_snap.get(gid) or {}).values():
                p = e.get("priceUsd")
                if not p or now - e.get("ts", 0) >= DEX_MAX_AGE_SEC:
                    continue
                if dom_ref > 0:
                    ratio = max(p, dom_ref) / max(min(p, dom_ref), 1e-12)
                    if ratio > DEX_MAX_RATIO:
                        continue          # different/dead token, not this one
                dexv.append(e)

        if len(cexv) + len(dexv) < 2:
            continue

        # Unified venue list. Each: buy = cost to acquire, sell = proceeds.
        venues = []
        for q in cexv:
            venues.append({"kind": "cex", "eid": q["eid"], "sym": q["symbol"],
                           "pair": q["pair"], "bid": q["bid"], "ask": q["ask"],
                           "buy": q["ask"], "sell": q["bid"]})
        for e in dexv:
            venues.append({"kind": "dex", "chain": e.get("chain"),
                           "dex": e.get("dex"), "price": e["priceUsd"],
                           "liq": e.get("liquidity", 0.0), "url": e.get("url"),
                           "contract": e.get("contract"),
                           "buy": e["priceUsd"], "sell": e["priceUsd"]})
        if len(venues) < 2:
            continue

        buy = min(venues, key=lambda v: v["buy"])
        sell = max(venues, key=lambda v: v["sell"])
        if buy is sell or sell["sell"] <= buy["buy"]:
            continue
        # Require at least one CEX leg: a pure DEX(chainA)->DEX(chainB) "arb"
        # is two different on-chain tokens that just share a name, not a real
        # opportunity (the GalaxE bsc->solana 1e12% fakes).
        if buy["kind"] == "dex" and sell["kind"] == "dex":
            continue
        spread = (sell["sell"] - buy["buy"]) / buy["buy"] * 100
        if spread < min_spread_pct:
            continue

        def _lbl(v):
            return v["chain"] if v["kind"] == "dex" else v["eid"]
        alerts.append({
            "group": gid, "ckey": gid, "ticker": base_gid,
            "display": group.get("display", gid),
            "spread": spread, "buy": buy, "sell": sell,
            "route": f"{_lbl(buy)}→{_lbl(sell)}",
            "cex": cexv, "dex": dexv, "listings": group["listings"],
        })

    # Verify each candidate's DEX leg against DexScreener (concurrently, cached):
    # the OKX price may come from a phantom pool (no real on-chain market),
    # a wrong-token contract, or a dead/illiquid pool. OKX stays the price
    # source — DexScreener only confirms the leg is a real, liquid pool of THIS
    # token. Candidates that fail are dropped. No price ratios involved.
    if verifier is not None and alerts:
        async def _ok(a) -> bool:
            for leg in (a["buy"], a["sell"]):
                if leg["kind"] != "dex":
                    continue
                v = await verifier.verify(leg.get("chain"), leg.get("contract"),
                                          a["ticker"], a["display"])
                if not v:
                    return False
            return True
        verdicts = await asyncio.gather(*[_ok(a) for a in alerts])
        alerts = [a for a, ok in zip(alerts, verdicts) if ok]

    alerts.sort(key=lambda a: a["spread"], reverse=True)
    return alerts


async def scan_loop(cex_book: CexPriceBook, universe, on_alert,
                     min_spread_pct: float = 6.0,
                     interval_sec: int = 5,
                     max_age_sec: int = 30,
                     cooldown_sec: int = 300,
                     blacklist=None,
                     dex_book=None,
                     verifier=None,
                     stop_event: asyncio.Event | None = None) -> None:
    if stop_event is None:
        stop_event = asyncio.Event()
    last_alert: dict[str, float] = {}  # cluster key -> ts

    # Warmup: skip the first WARMUP_SEC seconds so WS feeds populate. Without
    # it, partially-loaded clusters look like 1-2 ex outliers, bypassing the
    # OUTLIER_MAX_EX guard and producing mega-fake-spread alerts at start.
    warmup_sec = int(os.getenv("SCAN_WARMUP_SEC", "60"))
    log.info("scanner: warmup %ds before first scan", warmup_sec)
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=warmup_sec)
        return
    except asyncio.TimeoutError:
        pass

    while not stop_event.is_set():
        await asyncio.sleep(interval_sec)
        if not universe.groups:
            continue
        dex_snap = await dex_book.snapshot() if dex_book is not None else None
        try:
            alerts = await scan_once(cex_book, universe, min_spread_pct,
                                      max_age_sec, blacklist=blacklist,
                                      dex_snap=dex_snap, verifier=verifier)
        except Exception as e:
            log.exception("scan err: %s", e)
            continue

        now = time.time()
        sent = 0
        for a in alerts:
            if sent >= MAX_ALERTS_PER_TICK:
                break
            ckey = a.get("ckey", a["group"])
            if now - last_alert.get(ckey, 0) < cooldown_sec:
                continue
            last_alert[ckey] = now
            try:
                await on_alert(a)
                sent += 1
            except Exception as e:
                log.warning("alerter err for %s: %s", a["group"], e)

        if alerts:
            top = alerts[0]
            log.info("scan: %d alert(s), top %s %.2f%% (%s)",
                     len(alerts), top["display"], top["spread"], top["route"])
            if os.getenv("DUMP_ALERTS"):
                import json
                rows = []
                for a in alerts:
                    b, s = a["buy"], a["sell"]
                    rows.append({
                        "gid": a["group"], "display": a["display"],
                        "spread": round(a["spread"], 1), "route": a["route"],
                        "buy_kind": b["kind"],
                        "buy_id": b.get("eid") or b.get("chain"),
                        "buy_price": b["buy"], "buy_vol": b.get("liq"),
                        "sell_kind": s["kind"],
                        "sell_id": s.get("eid") or s.get("chain"),
                        "sell_price": s["sell"], "sell_vol": s.get("liq"),
                        "n_cex": len(a["cex"]), "n_dex": len(a["dex"]),
                    })
                try:
                    with open("alerts_debug.json", "w", encoding="utf-8") as f:
                        json.dump(rows, f, indent=1)
                except Exception:
                    pass
        else:
            log.info("scan: 0 alerts (universe groups=%d)", len(universe.groups))
