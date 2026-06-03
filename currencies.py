"""Periodic fetch_currencies() refresher per CEX, with per-exchange raw extractors
for exchanges where ccxt doesn't populate the unified `networks` field.

Cache layout (NORMALIZED exchange id — bingx#0..N collapsed to "bingx"):
  cache[exch_id][ASSET]["networks"][NETWORK_UPPER] =
      {"deposit": bool, "withdraw": bool, "fee": float|None, "contract": str|None}
"""
import asyncio
import hashlib
import hmac
import logging
import os
import random
import time

import aiohttp

log = logging.getLogger(__name__)

REFRESH_INTERVAL_SEC = int(os.getenv("CURRENCY_REFRESH_SEC", "1800"))
REST_TIMEOUT = aiohttp.ClientTimeout(total=15)
RATE_LIMIT_MARKERS = ("100410", "frequency", "rate limit", "ratelimit",
                       "too many", "429")
RATE_LIMIT_MAX_RETRIES = 5
RATE_LIMIT_BACKOFF_SEC = 2


def _norm(exch_id: str) -> str:
    """Strip shard suffix (bingx#0 -> bingx)."""
    return exch_id.partition("#")[0]


# ---------- contract extractors per "shape" of raw exchange data ----------

_CONTRACT_KEYS = (
    "contractAddress", "contract_address", "contract", "smartContractAddress",
    "tokenContractAddress", "address", "addr", "ctAddr",
)


def _scan_contract(d) -> str | None:
    if not isinstance(d, dict):
        return None
    for k in _CONTRACT_KEYS:
        v = d.get(k)
        if v and isinstance(v, str) and len(v) > 6:
            return v
    return None


# Match 0x..40-hex (EVM) and T..base58 (Tron) and solana base58 addresses out of URLs.
import re as _re
_URL_CONTRACT_RE = _re.compile(
    r"(0x[a-fA-F0-9]{40}|T[A-Za-z0-9]{33}|[1-9A-HJ-NP-Za-km-z]{32,44})"
)


def _addr_from_url(url: str | None) -> str | None:
    """Pull a contract address out of an explorer URL.

    coinex serves entries like:
       https://etherscan.io/token/0xdAC17F958D2ee523a2206206994597C13D831ec7
       https://bscscan.com/token/0x55d398...
       https://tronscan.org/#/token20/TR7NHqj...
    """
    if not url or not isinstance(url, str):
        return None
    m = _URL_CONTRACT_RE.search(url)
    return m.group(1) if m else None


def _extract_contract(ndata) -> str | None:
    """Best-effort contract extraction from a ccxt network entry."""
    if not ndata:
        return None
    # top-level keys
    c = _scan_contract(ndata)
    if c:
        return c
    raw = ndata.get("info")
    # nested raw
    c = _scan_contract(raw)
    if c:
        return c
    # coinex: contract lives only inside explorer_asset_url
    if isinstance(raw, dict):
        c = _addr_from_url(raw.get("explorer_asset_url"))
        if c:
            return c
    return None


# ---------- per-exchange raw extractors ----------
# Each returns a dict[NETWORK_UPPER, {deposit, withdraw, fee, contract}]
# or None when no useful info can be pulled.


def _extract_whitebit(info: dict) -> dict | None:
    """whitebit: top-level lacks networks dict; raw info has
    `networks.deposits/withdraws` arrays + `limits` per network."""
    raw = (info or {}).get("info") or {}
    nets_raw = raw.get("networks") or {}
    deposits = set(n.upper() for n in (nets_raw.get("deposits") or []))
    withdraws = set(n.upper() for n in (nets_raw.get("withdraws") or []))
    all_nets = deposits | withdraws
    if not all_nets:
        return None
    return {
        n: {"deposit": n in deposits, "withdraw": n in withdraws,
            "fee": None, "contract": None}
        for n in all_nets
    }


def _extract_kraken(info: dict) -> dict | None:
    """kraken's fetch_currencies returns empty networks for most assets;
    they expose 'status' (enabled/disabled) only at top level. We approximate
    with a single 'KRAKEN' bucket showing top-level active flag."""
    if not info:
        return None
    raw = info.get("info") or {}
    status = raw.get("status")
    active = info.get("active") if info.get("active") is not None else (status == "enabled")
    return {"KRAKEN": {"deposit": bool(active), "withdraw": bool(active),
                        "fee": None, "contract": None}}


def _extract_bitmart(info: dict) -> dict | None:
    """bitmart: only top-level deposit/withdraw bool, no per-network data."""
    raw = info.get("info") or {}
    d = raw.get("deposit_enabled")
    w = raw.get("withdraw_enabled")
    if d is None and w is None:
        d = info.get("deposit")
        w = info.get("withdraw")
    if d is None and w is None:
        return None
    return {"BITMART": {"deposit": bool(d), "withdraw": bool(w),
                         "fee": None, "contract": None}}


def _extract_phemex(info: dict) -> dict | None:
    """phemex: top-level deposit/withdraw flags."""
    d = info.get("deposit")
    w = info.get("withdraw")
    if d is None and w is None:
        return None
    return {"PHEMEX": {"deposit": bool(d), "withdraw": bool(w),
                        "fee": None, "contract": None}}


def _extract_coinbase(info: dict) -> dict | None:
    """coinbaseadvanced: ccxt returns no networks. Best signal: top-level active."""
    if info.get("active"):
        return {"COINBASE": {"deposit": True, "withdraw": True,
                              "fee": None, "contract": None}}
    return None


def _extract_exmo(info: dict) -> dict | None:
    """exmo: raw info is a list of {type, name, enabled, ...} entries per network."""
    raw = info.get("info")
    if not isinstance(raw, list):
        return None
    nets: dict[str, dict] = {}
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        name = (entry.get("name") or "").upper()
        if not name:
            continue
        typ = entry.get("type")
        enabled = bool(entry.get("enabled"))
        nets.setdefault(name, {"deposit": False, "withdraw": False,
                                "fee": None, "contract": None})
        if typ == "deposit":
            nets[name]["deposit"] = enabled
        elif typ == "withdraw":
            nets[name]["withdraw"] = enabled
    return nets or None


def _extract_ascendex(info: dict) -> dict | None:
    """ascendex: top-level status field 'Normal' means active."""
    raw = info.get("info") or {}
    status = (raw.get("status") or "").lower()
    active = status == "normal"
    return {"ASCENDEX": {"deposit": active, "withdraw": active,
                          "fee": None, "contract": None}}


_RAW_EXTRACTORS = {
    "whitebit": _extract_whitebit,
    "kraken": _extract_kraken,
    "bitmart": _extract_bitmart,
    "phemex": _extract_phemex,
    "coinbaseadvanced": _extract_coinbase,
    "exmo": _extract_exmo,
    "ascendex": _extract_ascendex,
}


def _gateio_chain_contract(ndata: dict, net_id: str) -> str | None:
    """gateio: info.chains[] is the per-currency chain list. ccxt may rename
    chains across the boundary (e.g. ETH -> ERC20), so match via the shared
    chain-normalization function used elsewhere.
    """
    raw = ndata.get("info") or {}
    if not isinstance(raw, dict):
        return None
    chains = raw.get("chains")
    if not isinstance(chains, list):
        return None
    # Lazy import to avoid circular import at module load.
    from alerter import cex_net_to_dex_chain
    target_dex = cex_net_to_dex_chain(net_id)
    target_upper = (net_id or "").upper()
    for c in chains:
        if not isinstance(c, dict):
            continue
        name = (c.get("name") or "").upper()
        if not name or not c.get("addr"):
            continue
        # 1. direct name match
        if name == target_upper:
            return c["addr"]
        # 2. dex_chain equivalence (e.g. ETH <-> ERC20 both map to ethereum)
        if target_dex and cex_net_to_dex_chain(name) == target_dex:
            return c["addr"]
    return None


def _normalize_currency_entry(exch_id: str, info: dict) -> dict[str, dict] | None:
    """Returns {NETWORK_UPPER: {deposit, withdraw, fee, contract}}.

    First tries ccxt's unified `networks` dict; falls back to per-exchange
    raw extractor when ccxt left it empty.
    """
    if not info:
        return None
    networks = info.get("networks") or {}
    if networks:
        out: dict[str, dict] = {}
        for net_id, ndata in networks.items():
            if not ndata or not net_id:
                continue
            contract = _extract_contract(ndata)
            # gateio: contract is only in info.chains[] keyed by chain name
            if not contract and exch_id == "gateio":
                contract = _gateio_chain_contract(ndata, net_id)
            out[str(net_id).upper()] = {
                "deposit": bool(ndata.get("deposit")),
                "withdraw": bool(ndata.get("withdraw")),
                "fee": ndata.get("fee"),
                "contract": contract,
            }
        if out:
            return out
    extractor = _RAW_EXTRACTORS.get(exch_id)
    if extractor:
        try:
            return extractor(info)
        except Exception as e:
            log.debug("[%s] raw extractor err: %s", exch_id, e)
    return None


def _is_rate_limited(err_str: str) -> bool:
    s = err_str.lower()
    return any(m in s for m in RATE_LIMIT_MARKERS)


async def _bingx_fetch_currencies_direct(inst, proxies: list[str]) -> dict | None:
    """Direct REST call to bingx /openApi/wallets/v1/capital/config/getall with
    per-request proxy rotation. ccxt's default path keeps hitting per-IP rate
    limits even though the endpoint is authenticated; rotating across our 1k
    proxy pool gives us fresh source IPs and side-steps the throttle.

    Returns the raw ccxt-shaped {code: {networks: {NET: ...}}} dict, or None.
    """
    if not inst.apiKey or not inst.secret:
        return None

    async with aiohttp.ClientSession() as session:
        last_err = ""
        for attempt in range(RATE_LIMIT_MAX_RETRIES):
            proxy = random.choice(proxies) if proxies else None
            ts = str(int(time.time() * 1000))
            qs = f"timestamp={ts}"
            sig = hmac.new(inst.secret.encode(), qs.encode(),
                             hashlib.sha256).hexdigest()
            url = (f"https://open-api.bingx.com/openApi/wallets/v1/"
                   f"capital/config/getall?{qs}&signature={sig}")
            try:
                async with session.get(url, proxy=proxy,
                                       headers={"X-BX-APIKEY": inst.apiKey},
                                       timeout=REST_TIMEOUT) as r:
                    data = await r.json(content_type=None)
            except Exception as e:
                last_err = str(e)
                await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                continue

            code = data.get("code")
            if code != 0:
                # bingx error envelope: {code: int, msg: str, data: null}
                msg = data.get("msg", "") or str(data)
                last_err = f"code={code} msg={msg[:80]}"
                if _is_rate_limited(last_err) or code in (100410, 100400, 429):
                    log.debug("bingx rate-limited (try %d) — rotating proxy",
                                attempt + 1)
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                    continue
                log.warning("bingx fetch_currencies API err: %s", last_err)
                return None
            items = data.get("data") or []
            return _bingx_parse(items)

        log.warning("bingx fetch_currencies exhausted %d retries: %s",
                    RATE_LIMIT_MAX_RETRIES, last_err[:120])
        return None


def _bingx_parse(items: list[dict]) -> dict:
    """Convert bingx /capital/config/getall response into ccxt-shaped dict
    {currency_code: {"networks": {network: {info, deposit, withdraw, ...}}}}.

    Output format matches what ccxt.fetch_currencies() would return so the rest
    of refresh_one's logic can process it uniformly.
    """
    out: dict[str, dict] = {}
    for coin in items:
        code = (coin.get("coin") or "").upper()
        if not code:
            continue
        networks: dict[str, dict] = {}
        for n in (coin.get("networkList") or []):
            net = (n.get("network") or "").strip()
            if not net:
                continue
            networks[net] = {
                "id": net,
                "network": net,
                "deposit": bool(n.get("depositEnable")),
                "withdraw": bool(n.get("withdrawEnable")),
                "fee": float(n["withdrawFee"]) if n.get("withdrawFee") else None,
                "info": {
                    "contractAddress": n.get("contractAddress") or "",
                    "depositMin": n.get("depositMin"),
                    "withdrawMin": n.get("withdrawMin"),
                    "minConfirm": n.get("minConfirm"),
                },
            }
        if networks:
            out[code] = {"networks": networks, "info": coin}
    return out


async def _lbank_fetch_currencies_direct(inst, proxies: list[str]) -> dict | None:
    """lbank /v2/withdrawConfigs.do — public endpoint, one batch with chain +
    canWithDraw + fee for all currencies. No deposit flag, no contract address.
    We still expose the withdraw flag + per-network listing which the alerter
    can show.
    """
    url = "https://api.lbkex.com/v2/withdrawConfigs.do"
    async with aiohttp.ClientSession() as session:
        last_err = ""
        for attempt in range(RATE_LIMIT_MAX_RETRIES):
            proxy = random.choice(proxies) if proxies else None
            try:
                async with session.get(url, proxy=proxy,
                                       timeout=REST_TIMEOUT) as r:
                    data = await r.json(content_type=None)
            except Exception as e:
                last_err = str(e)
                await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                continue
            items = data.get("data") if isinstance(data, dict) else None
            if not isinstance(items, list):
                last_err = f"unexpected payload: {str(data)[:120]}"
                if _is_rate_limited(last_err):
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                    continue
                log.warning("lbank withdrawConfigs err: %s", last_err)
                return None
            return _lbank_parse(items)
        log.warning("lbank withdrawConfigs exhausted retries: %s", last_err[:120])
        return None


def _lbank_parse(items: list[dict]) -> dict:
    out: dict[str, dict] = {}
    for row in items:
        code = (row.get("assetCode") or "").upper()
        if not code:
            continue
        chain = (row.get("chain") or "").upper()
        if not chain:
            continue
        entry = out.setdefault(code, {"networks": {}, "info": {}})
        entry["networks"][chain] = {
            "id": chain,
            "network": chain,
            # lbank's public endpoint exposes withdraw-side only. Assume deposit
            # mirrors withdraw — most tokens have either both open or both shut.
            "deposit": bool(row.get("canWithDraw")),
            "withdraw": bool(row.get("canWithDraw")),
            "fee": float(row["fee"]) if row.get("fee") not in (None, "") else None,
            "info": {
                "min": row.get("min"),
                "minTransfer": row.get("minTransfer"),
                # no contractAddress in this endpoint
            },
        }
    return out


async def _bitmart_fetch_currencies_direct(inst, proxies: list[str]) -> dict | None:
    """bitmart public /account/v1/currencies — flat list of
    {currency, network, contract_address, deposit_enabled, withdraw_enabled,
    withdraw_fee}. ccxt's bitmart only surfaces a top-level deposit/withdraw
    bool with no contract; this endpoint has the full per-network breakdown.
    Public, so proxy rotation works.
    """
    url = "https://api-cloud.bitmart.com/account/v1/currencies"
    async with aiohttp.ClientSession() as session:
        last_err = ""
        for attempt in range(RATE_LIMIT_MAX_RETRIES):
            proxy = random.choice(proxies) if proxies else None
            try:
                async with session.get(url, proxy=proxy, timeout=REST_TIMEOUT) as r:
                    data = await r.json(content_type=None)
            except Exception as e:
                last_err = str(e)
                await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                continue
            items = ((data or {}).get("data") or {}).get("currencies")
            if not isinstance(items, list):
                last_err = f"unexpected payload: {str(data)[:120]}"
                if _is_rate_limited(last_err):
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                    continue
                log.warning("bitmart currencies err: %s", last_err)
                return None
            out: dict[str, dict] = {}
            for it in items:
                cur = (it.get("currency") or "")
                # bitmart uses "TOKEN" or "TOKEN-NETWORK"
                code = cur.split("-")[0].upper()
                if not code:
                    continue
                net = (it.get("network") or cur).upper()
                entry = out.setdefault(code, {"networks": {}, "info": {}})
                entry["networks"][net] = {
                    "id": net, "network": net,
                    "deposit": bool(it.get("deposit_enabled")),
                    "withdraw": bool(it.get("withdraw_enabled")),
                    "fee": float(it["withdraw_fee"]) if it.get("withdraw_fee") else None,
                    "info": {"contractAddress": it.get("contract_address") or ""},
                }
            return out
        log.warning("bitmart currencies exhausted retries: %s", last_err[:120])
        return None


async def _coinbase_fetch_currencies_direct(inst, proxies: list[str]) -> dict | None:
    """Coinbase Exchange public /currencies — list of
    {id, status, supported_networks:[{name/id, status, contract_address,
    min_withdrawal_amount, network_confirmations}]}. ccxt's coinbaseadvanced
    needs auth and still omits contracts; this public endpoint has them.
    """
    url = "https://api.exchange.coinbase.com/currencies"
    async with aiohttp.ClientSession() as session:
        last_err = ""
        for attempt in range(RATE_LIMIT_MAX_RETRIES):
            proxy = random.choice(proxies) if proxies else None
            try:
                async with session.get(url, proxy=proxy,
                                       headers={"User-Agent": "Mozilla/5.0"},
                                       timeout=REST_TIMEOUT) as r:
                    data = await r.json(content_type=None)
            except Exception as e:
                last_err = str(e)
                await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                continue
            if not isinstance(data, list):
                last_err = f"unexpected payload: {str(data)[:120]}"
                if _is_rate_limited(last_err):
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                    continue
                log.warning("coinbase currencies err: %s", last_err)
                return None
            out: dict[str, dict] = {}
            for it in data:
                sym = (it.get("id") or "").upper()
                if not sym:
                    continue
                overall_online = (it.get("status") or "").lower() == "online"
                nets: dict[str, dict] = {}
                for n in (it.get("supported_networks") or []):
                    nm = (n.get("name") or n.get("id") or "").upper()
                    if not nm:
                        continue
                    online = (n.get("status") or "").lower() == "online"
                    nets[nm] = {
                        "id": nm, "network": nm,
                        "deposit": online and overall_online,
                        "withdraw": online and overall_online,
                        "fee": None,
                        "info": {"contractAddress": n.get("contract_address") or ""},
                    }
                if nets:
                    out[sym] = {"networks": nets, "info": it}
            return out
        log.warning("coinbase currencies exhausted retries: %s", last_err[:120])
        return None


# Per-exchange override: returns raw ccxt-shaped currencies dict, or None.
_DIRECT_FETCHERS = {
    "bingx": _bingx_fetch_currencies_direct,
    "lbank": _lbank_fetch_currencies_direct,
    "bitmart": _bitmart_fetch_currencies_direct,
    "coinbaseadvanced": _coinbase_fetch_currencies_direct,
}


class CurrencyCache:
    def __init__(self, proxies: list[str] | None = None):
        # cache[normalized_exchange_id][ASSET] = {"networks": {NET: {...}}, "ts": float}
        self.cache: dict[str, dict[str, dict]] = {}
        # names[normalized_exchange_id][ASSET] = project name (for universe grouping)
        self.names: dict[str, dict[str, str]] = {}
        self.lock = asyncio.Lock()
        # Loaded once at startup; used for per-call proxy rotation on rate-limited
        # currencies endpoints (notably bingx).
        if proxies is None:
            try:
                from exchanges import load_proxies
                proxies = load_proxies()
            except Exception:
                proxies = []
        self.proxies = proxies or []
        log.info("CurrencyCache: %d proxies available for rotation", len(self.proxies))

    async def _ccxt_fetch_with_retries(self, inst, norm_id: str):
        """ccxt fetch_currencies with rate-limit retries.

        exchanges.py monkey-patches inst.fetch_currencies to a no-op so that
        ccxt's load_markets doesn't trigger authed currency calls (the bingx
        100410 trap). The real method is stashed at inst._real_fetch_currencies
        — call that here, and flip options['fetchCurrencies']=True so guards
        inside the implementation (binance, htx, ...) don't short-circuit.
        """
        real_fetch = getattr(inst, "_real_fetch_currencies", None) or inst.fetch_currencies
        prev = inst.options.get("fetchCurrencies")
        inst.options["fetchCurrencies"] = True
        try:
            last_err = ""
            for attempt in range(RATE_LIMIT_MAX_RETRIES):
                try:
                    return await real_fetch()
                except Exception as e:
                    last_err = str(e)
                    if not _is_rate_limited(last_err):
                        raise
                    log.debug("[%s] ccxt rate-limited (try %d)", norm_id, attempt + 1)
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC * (attempt + 1))
            raise RuntimeError(f"rate-limited after {RATE_LIMIT_MAX_RETRIES} retries: {last_err[:120]}")
        finally:
            if prev is None:
                inst.options.pop("fetchCurrencies", None)
            else:
                inst.options["fetchCurrencies"] = prev

    async def refresh_one(self, exch_id: str, inst) -> None:
        if not inst.has.get("fetchCurrencies"):
            return
        norm_id = _norm(exch_id)
        currs = None
        # Direct REST path for exchanges with known rate-limit problems.
        direct = _DIRECT_FETCHERS.get(norm_id)
        if direct is not None:
            currs = await direct(inst, self.proxies)
        if currs is None:
            # Fallback to ccxt's built-in fetch_currencies (also tolerates rate
            # limits with retries + proxy rotation).
            try:
                currs = await self._ccxt_fetch_with_retries(inst, norm_id)
            except Exception as e:
                log.warning("[%s] fetch_currencies err: %s", exch_id, str(e)[:120])
                return
        if not currs:
            return
        norm_id = _norm(exch_id)
        result: dict[str, dict] = {}
        names: dict[str, str] = {}
        for code, info in currs.items():
            if not code or not info:
                continue
            nets = _normalize_currency_entry(norm_id, info)
            if not nets:
                continue
            ccode = str(code).upper()
            result[ccode] = {"networks": nets, "ts": time.time()}
            # project name for universe grouping: top-level "name", else raw info.name
            nm = info.get("name") if isinstance(info, dict) else None
            if not nm:
                raw = info.get("info") if isinstance(info, dict) else None
                if isinstance(raw, dict):
                    nm = raw.get("name") or raw.get("fullName")
            # Drop only a pure uppercase echo of the ticker (e.g. bybit
            # name "USDT" for USDT). A mixed-case real word like "Ronin"
            # for symbol RONIN must be kept — it's the grouping signal.
            if nm and str(nm).strip() and str(nm).strip() != ccode:
                names[ccode] = str(nm).strip()
        async with self.lock:
            self.cache[norm_id] = result
            self.names[norm_id] = names
        log.info("[%s] currencies refreshed: %d assets (%d named)",
                 norm_id, len(result), len(names))

    def names_snapshot(self) -> dict:
        """{eid: {ASSET: name}} — used by universe builder."""
        return {eid: dict(d) for eid, d in self.names.items()}

    async def refresh_loop(self, exchanges: dict, stop_event: asyncio.Event) -> None:
        """Two-tier refresh:
        - Main full pass every REFRESH_INTERVAL_SEC.
        - Rescue pass every RESCUE_INTERVAL_SEC, retrying only exchanges that
          have no cache yet (e.g. bingx after its transient cool-down).
        """
        rescue_interval = int(os.getenv("CURRENCY_RESCUE_SEC", "300"))
        while not stop_event.is_set():
            # Deduplicate shards.
            seen: set[str] = set()
            unique: list[tuple[str, object]] = []
            for eid, inst in exchanges.items():
                nid = _norm(eid)
                if nid in seen:
                    continue
                seen.add(nid)
                unique.append((eid, inst))
            tasks = [self.refresh_one(eid, inst) for eid, inst in unique]
            await asyncio.gather(*tasks, return_exceptions=True)
            total = sum(len(v) for v in self.cache.values())
            log.info("currency cache: %d exchanges, %d assets total",
                     len(self.cache), total)

            # Rescue passes: keep retrying exchanges with empty cache on a faster
            # cadence until the next full refresh window elapses.
            elapsed = 0
            while elapsed < REFRESH_INTERVAL_SEC and not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=rescue_interval)
                    return  # stop signalled
                except asyncio.TimeoutError:
                    pass
                elapsed += rescue_interval
                missing = [(eid, inst) for eid, inst in unique
                           if not self.cache.get(_norm(eid))]
                if not missing:
                    continue
                missing_ids = [_norm(eid) for eid, _ in missing]
                log.info("currency rescue: retrying %d failed exchange(s): %s",
                         len(missing), ",".join(missing_ids))
                tasks = [self.refresh_one(eid, inst) for eid, inst in missing]
                await asyncio.gather(*tasks, return_exceptions=True)
                total = sum(len(v) for v in self.cache.values())
                log.info("currency cache after rescue: %d exchanges, %d assets total",
                         len(self.cache), total)

    async def get(self, exch_id: str, asset: str) -> dict | None:
        async with self.lock:
            return self.cache.get(_norm(exch_id), {}).get(asset.upper())
