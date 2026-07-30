"""Token universe builder for CEX-CEX arbitrage.

Grouping rule (two passes via Union-Find):

  Pass 1 — TICKER: same uppercase ticker on different exchanges = same asset.
    All BTC listings unite; all WBTC listings unite; etc.

  Pass 2 — CROSS-TICKER ALIAS via EXACT LOWERCASED NAME: when two exchanges
    label the same on-chain asset with different display tickers — like
    Ronin's native token traded as "RON" on some CEX and "RONIN" on others —
    union them. Both have project name "Ronin", a raw exact match. Crucially
    NO stopword stripping: "Wrapped Ronin" stays distinct from "Ronin", and
    "Ethena USD" / "World Liberty Financial USD" stay distinct from "Ethena"
    / "World Liberty Financial" — those are different on-chain assets that
    just share part of the name.

Group id = most common ticker in the merged component. Display = first
non-empty project name. Contract + network data are carried per-listing for
the alert but do NOT drive grouping (a token can have different contracts
across networks but still be the same asset).

No CoinGecko, no median price filter — both rejected by design.
"""
import asyncio
import json
import logging
import os
import time

from dex_watcher import _norm_net, _norm_contract

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
UNIVERSE_FILE = os.path.join(_HERE, "token_universe.json")
REBUILD_INTERVAL_SEC = int(os.getenv("UNIVERSE_REBUILD_SEC", str(3600)))  # 1h
UNIVERSE_MAX_AGE_SEC = int(os.getenv("UNIVERSE_MAX_AGE_SEC", str(48 * 3600)))

# Ticker prefixes that indicate a wrapper/staking variant — used for the
# alert's "wrap_kind" badge ONLY, not for grouping. Longest-first.
_WRAP_PREFIXES = ("WST", "BST", "WB", "ST", "WS", "BB", "BE", "W", "S", "B", "M", "R")
_MIN_BASE_LEN = 3  # don't strip past "SUI" -> "UI"


def wrap_info(symbol: str) -> tuple[str, str | None]:
    """Return (base_symbol, wrap_kind). wrap_kind in {w, st, s, ...} or None.
    Display-only: the scanner does NOT group wrappers with their bases."""
    su = symbol.upper()
    for pfx in _WRAP_PREFIXES:
        if su.startswith(pfx) and len(su) - len(pfx) >= _MIN_BASE_LEN:
            return su[len(pfx):], pfx.lower()
    return su, None


def _norm(exch_id: str) -> str:
    return exch_id.partition("#")[0]


class _UF:
    """Union-Find over listing indices."""
    def __init__(self, n: int):
        self.p = list(range(n))

    def find(self, x: int) -> int:
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.p[rb] = ra


def _norm_key(name: str) -> str:
    """Lowercase + strip 'wrapped' token. 'Wrapped Ronin' → 'ronin'."""
    return " ".join(t for t in name.strip().lower().split() if t != "wrapped")


def _listing_cc(lst) -> list[tuple[str, str]]:
    """Listing's (normalized_chain, normalized_contract) pairs."""
    out = []
    for net, nd in (lst.get("networks") or {}).items():
        c = _norm_contract(nd.get("contract") or "")
        if c and len(c) > 6:
            out.append((_norm_net(net) or "?" + (net or "").lower(), c))
    return out


def _is_wrap_pair(t1: set, t2: set) -> bool:
    """True if the two ticker-sets are a strict wrapper pair (one ticker is
    exactly 'W'+the other, e.g. BTC/WBTC, ETH/WETH, DOG/WDOG). Those genuinely
    track the same asset 1:1 and stay merged; everything else (SAFE/SAFE4,
    AI/AIGENSYN, RENDER/RNDR) is a different token and must split."""
    for a in t1:
        for b in t2:
            lo, hi = (a, b) if len(a) <= len(b) else (b, a)
            if len(lo) >= 3 and hi == "W" + lo:
                return True
    return False


def _build_groups(listings: list[dict],
                    cg_ctr_index: dict[tuple[str, str], str] | None = None
                    ) -> list[list[int]]:
    """Contract-identity grouping with a same-chain contract-conflict guard.

      Pass A  — CONTRACT: listings sharing a contract address are the same
        token (handles multichain: each exchange's listing carries all its
        networks, so they share at least one contract).
      Pass A' — CG_CROSS_CHAIN (new): CoinGecko authoritative multichain
        mapping. If two listings' contracts appear in the same CG coin's
        platforms dict (e.g. WETH on ethereum vs base, USDC on 15 chains),
        they belong to the same project and get merged — even when the
        contract strings themselves differ across chains.
      Pass B  — TICKER (guarded) and Pass C — NAME_KEY (guarded): merge by
        ticker / project-name, BUT never merge two components that hold
        DIFFERENT contracts on the SAME chain — those are different projects
        sharing a ticker (the two BSC "DUCKY"s; Gensyn vs SleepAI). They stay
        as separate groups. NAME merges RON+RONIN+Wrapped-Ronin ('ronin') and
        keeps 'Ethena' vs 'Ethena USD' apart.

      FORBIDDEN-PAIR guard: an exchange is authoritative about its own
        listings — if one exchange lists contract C1 and contract C2 under
        DIFFERENT tickers (e.g. mexc: 0x5afe='SAFE', 0x4d7f='SAFE4'), then C1
        and C2 are different tokens and must NEVER share a group, no matter
        what other merges suggest. This kills the cross-token DEX fakes (a
        wrong cross-chain contract priced as the group's token). Exception:
        strict wrappers (W+base) stay merged. No prices involved.
    """
    n = len(listings)
    uf = _UF(n)

    # Forbidden contract pairs: built from each exchange's own ticker->contract
    # assignments. If an exchange gives two contracts disjoint ticker sets,
    # they are different tokens (unless a strict W+base wrapper pair).
    ex_cmap: dict[str, dict[str, set]] = {}   # eid -> {contract -> set(tickers)}
    for lst in listings:
        eid = lst["eid"]; tk = lst["symbol"].upper()
        for _, c in _listing_cc(lst):
            ex_cmap.setdefault(eid, {}).setdefault(c, set()).add(tk)
    forbidden_adj: dict[str, set] = {}          # contract -> set of contracts it must NOT merge with
    for cmap in ex_cmap.values():
        items = list(cmap.items())
        for x in range(len(items)):
            for y in range(x + 1, len(items)):
                c1, t1 = items[x]; c2, t2 = items[y]
                if t1 & t2:
                    continue                    # shared ticker -> same token (alias)
                if _is_wrap_pair(t1, t2):
                    continue                    # BTC/WBTC etc. -> keep mergeable
                forbidden_adj.setdefault(c1, set()).add(c2)
                forbidden_adj.setdefault(c2, set()).add(c1)

    # Pass A: union by shared contract address.
    by_contract: dict[str, int] = {}
    for i, lst in enumerate(listings):
        for _, c in _listing_cc(lst):
            if c in by_contract:
                uf.union(by_contract[c], i)
            else:
                by_contract[c] = i

    # Per-root chain -> set(contract) (same-chain conflicts) and the flat
    # set of all contracts in the component (forbidden-pair check).
    root_cc: dict[int, dict[str, set]] = {}
    root_all: dict[int, set] = {}
    for i, lst in enumerate(listings):
        r = uf.find(i)
        d = root_cc.setdefault(r, {})
        a = root_all.setdefault(r, set())
        for ch, c in _listing_cc(lst):
            d.setdefault(ch, set()).add(c)
            a.add(c)

    def _conflict(a: int, b: int) -> bool:
        ra, rb = uf.find(a), uf.find(b)
        if ra == rb:
            return False
        da, db = root_cc.get(ra, {}), root_cc.get(rb, {})
        for ch, cs in da.items():
            ocs = db.get(ch)
            if cs and ocs and not (cs & ocs):
                return True       # same chain, disjoint contracts -> different projects
        # forbidden-pair: any contract here paired with any contract there that
        # some exchange marked as a different token -> never merge.
        if forbidden_adj:
            aa, ab = root_all.get(ra), root_all.get(rb)
            if aa and ab:
                if len(ab) < len(aa):
                    aa, ab = ab, aa
                for c1 in aa:
                    partners = forbidden_adj.get(c1)
                    if partners and (partners & ab):
                        return True
        return False

    def _gunion(a: int, b: int) -> None:
        if _conflict(a, b):
            return
        ra, rb = uf.find(a), uf.find(b)
        if ra == rb:
            return
        uf.union(a, b)
        nr = uf.find(a)
        old = rb if nr == ra else ra
        dst = root_cc.setdefault(nr, {})
        for ch, cs in root_cc.get(old, {}).items():
            dst.setdefault(ch, set()).update(cs)
        root_all.setdefault(nr, set()).update(root_all.get(old, set()))

    # Pass A': CG cross-chain identity — union listings sharing a CG coin id.
    if cg_ctr_index:
        by_cg: dict[str, int] = {}
        cg_merges = 0
        for i, lst in enumerate(listings):
            hit_id: str | None = None
            for ch, c in _listing_cc(lst):
                key = (ch, c.lower())
                cg_id = cg_ctr_index.get(key)
                if cg_id:
                    hit_id = cg_id
                    break
            if not hit_id:
                continue
            if hit_id in by_cg:
                # _gunion respects conflict + forbidden-pair guards
                prev = by_cg[hit_id]
                if uf.find(prev) != uf.find(i):
                    _gunion(prev, i)
                    cg_merges += 1
            else:
                by_cg[hit_id] = i
        if cg_merges:
            log.info("universe: Pass A' CG cross-chain merged %d listing pairs "
                     "(%d unique cg_ids)", cg_merges, len(by_cg))

    # Pass B: ticker (guarded).
    by_ticker: dict[str, int] = {}
    for i, lst in enumerate(listings):
        t = lst["symbol"].upper()
        if t in by_ticker:
            _gunion(by_ticker[t], i)
        else:
            by_ticker[t] = i

    # Pass C: project name_key (guarded).
    by_name: dict[str, int] = {}
    for i, lst in enumerate(listings):
        nm = _norm_key(lst.get("name") or "")
        if len(nm) < 2:
            continue
        if nm in by_name:
            _gunion(by_name[nm], i)
        else:
            by_name[nm] = i

    comps: dict[int, list[int]] = {}
    for i in range(n):
        comps.setdefault(uf.find(i), []).append(i)
    return list(comps.values())


class TokenUniverse:
    def __init__(self):
        self.groups: dict[str, dict] = {}
        self.index: dict[tuple[str, str], str] = {}
        self.built_ts: float = 0.0

    def all_tickers(self) -> set[str]:
        """Set of every base symbol across all listings — used by
        cex_watcher to filter exchange markets to only universe tokens."""
        out: set[str] = set()
        for g in self.groups.values():
            for lst in g.get("listings", []):
                s = (lst.get("symbol") or "").upper()
                if s:
                    out.add(s)
        return out

    # ---------- build ----------

    def build_from_cache(self, currency_cache, ticker_names: dict) -> set[str]:
        """Rebuild groups from currency_cache.

        Returns the set of NEW group ids (present after rebuild, absent before).
        Empty set on first build or when nothing changed. Used by the alerter to
        push TG notifications about freshly-listed tokens.

        Safety: if the current cache covers FEWER exchanges than the existing
        universe was built from, we KEEP the existing universe rather than
        nuke it. Prevents transient fetch_currencies failures (proxy timeout,
        expired API key) from wiping out otherwise-valid group data."""
        previous_gids = set(self.groups.keys())
        cache_eids = {_norm(e) for e in currency_cache.cache
                      if currency_cache.cache.get(e)}
        prev_eids = {lst["eid"] for g in self.groups.values()
                     for lst in g.get("listings", [])}
        if prev_eids and len(cache_eids) < len(prev_eids):
            log.warning("universe: cache has %d exchanges vs universe's %d "
                        "— preserving existing universe (missing: %s)",
                        len(cache_eids), len(prev_eids),
                        ",".join(sorted(prev_eids - cache_eids)))
            return set()
        listings: list[dict] = []
        for eid, assets in currency_cache.cache.items():
            eid = _norm(eid)
            names = ticker_names.get(eid, {})
            for asset, info in assets.items():
                listings.append({
                    "eid": eid,
                    "symbol": asset,
                    "name": names.get(asset, ""),
                    "networks": info.get("networks") or {},
                })
        if not listings:
            log.warning("universe: no listings in currency cache")
            return set()

        # Build (chain, contract) -> canonical_id index for cross-chain merging.
        # Union CoinGecko + CoinMarketCap. If both know the same coin, CG wins
        # (its data is denser and machine-readable). Any exchange listing whose
        # contract is in this index gets merged with all other listings sharing
        # the same canonical_id — same project across different chains.
        cg_ctr_index: dict[tuple[str, str], str] = {}
        try:
            import coingecko
            coins, _ts = coingecko.load()
            if coins:
                cg_ctr_index = coingecko.build_contract_index(coins)
                log.info("universe: CG contract index loaded (%d entries)",
                         len(cg_ctr_index))
        except Exception as e:
            log.warning("universe: CG contract index load err: %s", e)
        try:
            import coinmarketcap
            cmc_coins, _ts = coinmarketcap.load()
            if cmc_coins:
                cmc_ctr = coinmarketcap.build_contract_index(cmc_coins)
                added = 0
                for k, cmc_id in cmc_ctr.items():
                    if k not in cg_ctr_index:
                        cg_ctr_index[k] = f"cmc:{cmc_id}"
                        added += 1
                log.info("universe: CMC contract index +%d entries (total %d)",
                         added, len(cg_ctr_index))
        except Exception as e:
            log.warning("universe: CMC contract index load err: %s", e)

        comps = _build_groups(listings, cg_ctr_index=cg_ctr_index or None)
        groups: dict[str, dict] = {}
        index: dict[tuple[str, str], str] = {}
        for comp in comps:
            members = [listings[i] for i in comp]
            eids = {m["eid"] for m in members}
            has_contract = any((nd.get("contract") or "")
                               for m in members
                               for nd in (m.get("networks") or {}).values())
            # Keep a group if it's arbitrable: either >=2 CEX (CEX<->CEX), or a
            # single CEX listing that has a contract (CEX<->DEX needs just one
            # CEX leg + the on-chain pool). Drop single-CEX with no contract —
            # can't pair it with anything.
            if len(eids) < 2 and not has_contract:
                continue
            # gid stays ticker-based — cex_book & existing lookups depend on it.
            # cg_id lives on the group (set by _augment_from_coingecko later).
            tickers = [m["symbol"].upper() for m in members]
            gid = max(set(tickers), key=tickers.count)
            # Precompute the group's cg_id from any contract that matches CG
            # — used by Pass A' cross-chain merging (already done) and shown
            # on the group so /augment doesn't have to rediscover it.
            cg_hit: str | None = None
            if cg_ctr_index:
                from dex_watcher import _norm_net as _nn
                for m in members:
                    for ch, nd in (m.get("networks") or {}).items():
                        c = (nd.get("contract") or "").strip().lower()
                        if not c:
                            continue
                        ch_n = _nn(ch) or ch.lower()
                        h = cg_ctr_index.get((ch_n, c))
                        if h:
                            cg_hit = h
                            break
                    if cg_hit:
                        break
            # de-dupe key collisions across truly different components
            gkey = gid
            suffix = 0
            while gkey in groups:
                suffix += 1
                gkey = f"{gid}#{suffix}"
            display = next((m["name"] for m in members if m["name"]), gkey)
            lst_out = []
            for m in members:
                _, kind = wrap_info(m["symbol"])
                lst_out.append({
                    "eid": m["eid"],
                    "symbol": m["symbol"],
                    "name": m["name"],
                    "wrapped": kind is not None,
                    "wrap_kind": kind,
                    "networks": m["networks"],
                })
                index[(m["eid"], m["symbol"].upper())] = gkey
            group_entry = {"display": display, "listings": lst_out}
            if cg_hit:
                group_entry["cg_id"] = cg_hit
            groups[gkey] = group_entry
        self.groups = groups
        self.index = index
        self.built_ts = time.time()
        log.info("universe built: %d groups from %d listings (%d exchanges)",
                 len(groups), len(listings),
                 len({l['eid'] for l in listings}))
        # Augment with CoinGecko: for each group, look up its ticker in the CG
        # bulk index and merge canonical multichain platforms as
        # group["cg_platforms"] = {chain: contract}. DexWatcher picks these up
        # in _refresh_jobs. Adds coverage the CEXes don't report (e.g. USDC on
        # 15 chains vs the 3-4 CEXes bother listing).
        try:
            self._augment_from_coingecko()
        except Exception as e:
            log.warning("universe: CG augment failed: %s", e)
        new_gids = set(self.groups.keys()) - previous_gids
        if previous_gids and new_gids:
            log.info("universe: %d NEW groups: %s",
                     len(new_gids), ",".join(sorted(new_gids)[:20]))
        return new_gids

    def _augment_from_coingecko(self) -> None:
        import coingecko
        from dex_watcher import _norm_contract
        coins, ts = coingecko.load()
        if not coins:
            log.info("universe: no CG index yet, skipping augment")
            return
        idx = coingecko.build_index(coins)
        matched = 0
        added_pairs = 0
        for gid, g in self.groups.items():
            base = gid.partition("#")[0]
            candidates = idx.get(base) or []
            if not candidates:
                continue
            # collect our existing (chain, contract-lower) so we can
            # (a) disambiguate colliding tickers by contract match
            # (b) not double-add chains we already have
            existing_pairs: set[tuple[str, str]] = set()
            existing_contracts: set[str] = set()
            for lst in g["listings"]:
                for net, nd in (lst.get("networks") or {}).items():
                    c = _norm_contract(nd.get("contract") or "")
                    if c and len(c) > 6:
                        cl = c.lower()
                        existing_contracts.add(cl)
                        chain = _norm_net(net) or net.lower()
                        existing_pairs.add((chain, cl))

            # disambiguate: prefer the CG entry that has any overlapping contract.
            picked: dict | None = None
            for cand in candidates:
                if any(a.lower() in existing_contracts
                       for a in cand["platforms"].values()):
                    picked = cand
                    break
            if picked is None:
                if len(candidates) == 1:
                    picked = candidates[0]
                else:
                    continue                                        # ambiguous, skip

            # merge platforms that aren't already covered
            cg_plats: dict[str, str] = {}
            for chain, addr in picked["platforms"].items():
                addr_l = addr.lower()
                if (chain, addr_l) in existing_pairs:
                    continue
                cg_plats[chain] = addr_l
                added_pairs += 1
            if cg_plats:
                g["cg_platforms"] = cg_plats
                g["cg_id"] = picked["id"]
                g["cg_name"] = picked["name"]
                matched += 1
        log.info("CG augment: %d groups matched, +%d (chain,contract) pairs",
                 matched, added_pairs)

    # ---------- persistence ----------

    def save(self) -> None:
        try:
            with open(UNIVERSE_FILE, "w", encoding="utf-8") as f:
                json.dump({"ts": self.built_ts, "groups": self.groups}, f)
            log.info("universe saved: %d groups", len(self.groups))
        except Exception as e:
            log.warning("universe save err: %s", e)

    def load(self) -> bool:
        try:
            with open(UNIVERSE_FILE, encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError:
            return False
        except Exception as e:
            log.warning("universe load err: %s", e)
            return False
        age = time.time() - data.get("ts", 0)
        if age > UNIVERSE_MAX_AGE_SEC:
            log.info("universe on disk is stale (%.1fh) — will rebuild", age / 3600)
            return False
        self.groups = data.get("groups", {})
        self.built_ts = data.get("ts", 0)
        self.index = {}
        for gid, g in self.groups.items():
            for lst in g["listings"]:
                self.index[(lst["eid"], lst["symbol"].upper())] = gid
        log.info("universe loaded from disk: %d groups (age %.1fh)",
                 len(self.groups), age / 3600)
        return True

    # ---------- rebuild loop ----------

    async def rebuild_loop(self, currency_cache, ticker_names_provider,
                            stop_event: asyncio.Event,
                            on_new_groups=None) -> None:
        """Periodic rebuild. If on_new_groups is provided, it's called with
        (new_gids: set[str], groups_snapshot: dict) after each rebuild that
        produced fresh entries — used by the alerter to push TG notifications."""
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(),
                                       timeout=REBUILD_INTERVAL_SEC)
                return
            except asyncio.TimeoutError:
                pass
            try:
                new_gids = self.build_from_cache(currency_cache,
                                                    ticker_names_provider())
                self.save()
                if new_gids and on_new_groups is not None:
                    try:
                        snap = {gid: self.groups[gid] for gid in new_gids
                                if gid in self.groups}
                        await on_new_groups(new_gids, snap)
                    except Exception as e:
                        log.warning("on_new_groups callback err: %s", e)
            except Exception as e:
                log.exception("universe rebuild err: %s", e)
