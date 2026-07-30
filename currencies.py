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


async def _http_get_json(session, url, proxies, headers=None, direct_first=True):
    """GET a JSON endpoint with 'direct first, proxy fallback' strategy.

    Direct connect is faster and more reliable than random-proxy roulette when
    the endpoint isn't geo-blocked. Only fall back to a rotating proxy if the
    direct call fails."""
    hdr = headers or {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
    tries = [None]                            # direct
    tries += [random.choice(proxies) if proxies else None
              for _ in range(RATE_LIMIT_MAX_RETRIES - 1)]
    if not direct_first:
        tries = tries[1:] + [None]            # proxy first, then direct
    last_err = ""
    for i, proxy in enumerate(tries):
        try:
            async with session.get(url, headers=hdr, proxy=proxy,
                                    timeout=REST_TIMEOUT) as r:
                if r.status != 200:
                    last_err = f"HTTP {r.status}"
                    continue
                return await r.json(content_type=None)
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)[:80]}"
            await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC if i > 0 else 0)
    log.warning("http_get_json %s exhausted %d tries: %s",
                url[:80], len(tries), last_err[:120])
    return None


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
    """Direct REST call to bingx /openApi/wallets/v1/capital/config/getall.

    Strategy: direct connection first (measured ~700ms, always returns full
    data). Rotating proxies as a last-resort fallback in case bingx ever
    starts rate-limiting our residential IP.
    """
    if not inst.apiKey or not inst.secret:
        return None

    def _sign_url():
        ts = str(int(time.time() * 1000))
        qs = f"timestamp={ts}"
        sig = hmac.new(inst.secret.encode(), qs.encode(),
                         hashlib.sha256).hexdigest()
        return (f"https://open-api.bingx.com/openApi/wallets/v1/"
                f"capital/config/getall?{qs}&signature={sig}")

    headers = {"X-BX-APIKEY": inst.apiKey}
    tries = [None]                            # direct first
    tries += [random.choice(proxies) if proxies else None
              for _ in range(RATE_LIMIT_MAX_RETRIES - 1)]
    async with aiohttp.ClientSession() as session:
        last_err = ""
        for i, proxy in enumerate(tries):
            url = _sign_url()                 # fresh signed URL per attempt
            try:
                async with session.get(url, proxy=proxy, headers=headers,
                                       timeout=REST_TIMEOUT) as r:
                    data = await r.json(content_type=None)
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:80]}"
                if i < len(tries) - 1:
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                continue
            code = data.get("code") if isinstance(data, dict) else None
            if code != 0:
                msg = (data.get("msg") if isinstance(data, dict) else "") or str(data)[:80]
                last_err = f"code={code} msg={msg[:80]}"
                if _is_rate_limited(last_err) or code in (100410, 100400, 429):
                    await asyncio.sleep(RATE_LIMIT_BACKOFF_SEC)
                    continue
                log.warning("bingx fetch_currencies API err: %s", last_err)
                return None
            items = data.get("data") or []
            return _bingx_parse(items)
    log.warning("bingx fetch_currencies exhausted %d tries: %s",
                len(tries), last_err[:120])
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
    """lbank /v2/withdrawConfigs.do — public per-chain flags + fees.
    No contract addresses (lbank doesn't publish those)."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://api.lbkex.com/v2/withdrawConfigs.do", proxies)
    items = data.get("data") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return None
    return _lbank_parse(items)


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
    withdraw_fee}."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://api-cloud.bitmart.com/account/v1/currencies",
            proxies)
    items = ((data or {}).get("data") or {}).get("currencies")
    if not isinstance(items, list):
        return None
    out: dict[str, dict] = {}
    for it in items:
        cur = (it.get("currency") or "")
        code = cur.split("-")[0].upper()
        if not code:
            continue
        net = (it.get("network") or cur).upper()
        entry = out.setdefault(code, {"networks": {}, "info": {"name": it.get("name")}})
        entry["networks"][net] = {
            "id": net, "network": net,
            "deposit": bool(it.get("deposit_enabled")),
            "withdraw": bool(it.get("withdraw_enabled")),
            "fee": float(it["withdraw_fee"]) if it.get("withdraw_fee") else None,
            "info": {"contractAddress": it.get("contract_address") or ""},
        }
    return out


async def _coinbase_fetch_currencies_direct(inst, proxies: list[str]) -> dict | None:
    """Coinbase Exchange public /currencies — has per-network contract
    addresses in `supported_networks[].contract_address`."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://api.exchange.coinbase.com/currencies", proxies)
    if not isinstance(data, list):
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
        # Fallback: no supported_networks but details.crypto_address_link has
        # a token URL with the contract embedded (single-chain assets).
        if not nets:
            link = (it.get("details") or {}).get("crypto_address_link", "")
            c = _addr_from_url(link)
            if c:
                nets["ETH"] = {  # default chain for unlabeled ETH tokens
                    "id": "ETH", "network": "ETH",
                    "deposit": overall_online, "withdraw": overall_online,
                    "fee": None,
                    "info": {"contractAddress": c},
                }
        if nets:
            out[sym] = {"networks": nets, "info": it}
    return out


async def _bitrue_fetch_currencies_direct(inst, proxies):
    """bitrue /api/v1/exchangeInfo — has `coins` array with per-chain flags.
    No contract addresses in the public payload."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://openapi.bitrue.com/api/v1/exchangeInfo", proxies)
    coins = (data or {}).get("coins")
    if not isinstance(coins, list):
        return None
    out = {}
    for c in coins:
        code = (c.get("coin") or "").upper()
        if not code: continue
        nets = {}
        for cd in c.get("chainDetail") or []:
            ch = (cd.get("chain") or "").upper()
            if not ch: continue
            nets[ch] = {
                "id": ch, "network": ch,
                "deposit": bool(cd.get("enableDeposit")),
                "withdraw": bool(cd.get("enableWithdraw")),
                "fee": float(cd["withdrawFee"]) if cd.get("withdrawFee") not in (None,"") else None,
                "info": {"contractAddress": ""},
            }
        if nets:
            out[code] = {"networks": nets, "info": {"name": c.get("coinFulName")}}
    return out


async def _hashkey_fetch_currencies_direct(inst, proxies):
    """hashkey /api/v1/exchangeInfo — coins[] with chainTypes[].
    No contracts published."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://api-glb.hashkey.com/api/v1/exchangeInfo", proxies)
    coins = (data or {}).get("coins")
    if not isinstance(coins, list):
        return None
    out = {}
    for c in coins:
        code = (c.get("coinId") or "").upper()
        if not code: continue
        overall_dep = bool(c.get("allowDeposit"))
        overall_wd = bool(c.get("allowWithdraw"))
        nets = {}
        for ct in c.get("chainTypes") or []:
            ch = (ct.get("chainType") or "").upper()
            if not ch: continue
            nets[ch] = {
                "id": ch, "network": ch,
                "deposit": bool(ct.get("allowDeposit", overall_dep)),
                "withdraw": bool(ct.get("allowWithdraw", overall_wd)),
                "fee": float(ct["withdrawFee"]) if ct.get("withdrawFee") not in (None,"") else None,
                "info": {"contractAddress": ""},
            }
        if nets:
            out[code] = {"networks": nets, "info": {"name": c.get("coinFullName") or c.get("coinName")}}
    return out


async def _phemex_fetch_currencies_direct(inst, proxies):
    """phemex /public/products — has currencies list. No per-chain data or
    contracts, so we emit a single PHEMEX bucket per currency."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://api.phemex.com/public/products", proxies)
    curs = ((data or {}).get("data") or {}).get("currencies")
    if not isinstance(curs, list):
        return None
    out = {}
    for c in curs:
        code = (c.get("currency") or "").upper()
        if not code: continue
        listed = (c.get("status") or "").lower() == "listed"
        out[code] = {
            "networks": {"PHEMEX": {
                "deposit": listed, "withdraw": listed,
                "fee": None, "info": {"contractAddress": ""},
            }},
            "info": {"name": c.get("name")},
        }
    return out


async def _bitstamp_fetch_currencies_direct(inst, proxies):
    """bitstamp /api/v2/currencies/ — has per-network flags but NO contracts."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://www.bitstamp.net/api/v2/currencies/", proxies)
    if not isinstance(data, list):
        return None
    out = {}
    for it in data:
        code = (it.get("currency") or "").upper()
        if not code: continue
        overall_dep = (it.get("deposit") or "").lower() == "enabled"
        overall_wd = (it.get("withdrawal") or "").lower() == "enabled"
        nets = {}
        for n in it.get("networks") or []:
            ch = (n.get("network") or "").upper()
            if not ch: continue
            nets[ch] = {
                "id": ch, "network": ch,
                "deposit": (n.get("deposit") or "").lower() == "enabled" and overall_dep,
                "withdraw": (n.get("withdrawal") or "").lower() == "enabled" and overall_wd,
                "fee": None, "info": {"contractAddress": ""},
            }
        # Fallback to a single bucket when no networks published (native coins)
        if not nets and (overall_dep or overall_wd):
            nets["BITSTAMP"] = {"deposit": overall_dep, "withdraw": overall_wd,
                                 "fee": None, "info": {"contractAddress": ""}}
        if nets:
            out[code] = {"networks": nets, "info": {"name": it.get("name")}}
    return out


async def _okx_fetch_currencies_direct(inst, proxies):
    """okx /api/v5/asset/currencies — private, needs API key. Returns per-chain
    ccy/chain/contract mapping. ccxt.okx().load_markets() crashes on a
    parse_market NoneType bug, so we can't rely on ccxt for this exchange."""
    if not (inst.apiKey and inst.secret and getattr(inst, "password", None)):
        return None
    import base64, hmac as _hmac, hashlib as _hashlib, datetime as _dt
    ts = _dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.") + \
         f"{_dt.datetime.utcnow().microsecond // 1000:03d}Z"
    path = "/api/v5/asset/currencies"
    prehash = ts + "GET" + path
    sig = base64.b64encode(_hmac.new(inst.secret.encode(), prehash.encode(),
                                       _hashlib.sha256).digest()).decode()
    headers = {
        "OK-ACCESS-KEY": inst.apiKey,
        "OK-ACCESS-SIGN": sig,
        "OK-ACCESS-TIMESTAMP": ts,
        "OK-ACCESS-PASSPHRASE": inst.password,
        "User-Agent": "Mozilla/5.0",
    }
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://www.okx.com" + path, proxies, headers=headers)
    items = (data or {}).get("data") if isinstance(data, dict) else None
    if not isinstance(items, list):
        log.warning("okx currencies: unexpected payload: %s", str(data)[:120])
        return None
    out: dict[str, dict] = {}
    for it in items:
        code = (it.get("ccy") or "").upper()
        if not code: continue
        chain_full = it.get("chain") or ""      # e.g. "USDT-Ethereum"
        # chain field is "<ccy>-<network>", strip ccy prefix
        ch = chain_full.split("-", 1)[1] if "-" in chain_full else chain_full
        ch = ch.upper() or "OKX"
        entry = out.setdefault(code, {"networks": {}, "info": {"name": it.get("name")}})
        entry["networks"][ch] = {
            "id": ch, "network": ch,
            "deposit": bool(it.get("canDep")),
            "withdraw": bool(it.get("canWd")),
            "fee": float(it["minFee"]) if it.get("minFee") not in (None,"") else None,
            "info": {"contractAddress": it.get("ctAddr") or ""},
        }
    return out


async def _cryptocom_fetch_currencies_direct(inst, proxies):
    """cryptocom /exchange/v1/public/get-currencies — publishes tradable
    currencies but no networks/contracts. We emit a single CRYPTOCOM bucket
    per currency."""
    async with aiohttp.ClientSession() as session:
        data = await _http_get_json(
            session, "https://api.crypto.com/exchange/v1/public/get-currencies",
            proxies)
    items = ((data or {}).get("result") or {}).get("data")
    if not isinstance(items, list):
        return None
    out = {}
    seen = set()
    for it in items:
        code = (it.get("base_ccy") or "").upper()
        if not code or code in seen: continue
        seen.add(code)
        active = bool(it.get("tradable"))
        out[code] = {
            "networks": {"CRYPTOCOM": {
                "deposit": active, "withdraw": active,
                "fee": None, "info": {"contractAddress": ""},
            }},
            "info": {"name": it.get("display_name")},
        }
    return out


# Per-exchange override: returns raw ccxt-shaped currencies dict, or None.
_DIRECT_FETCHERS = {
    "bingx": _bingx_fetch_currencies_direct,
    "lbank": _lbank_fetch_currencies_direct,
    "bitmart": _bitmart_fetch_currencies_direct,
    "coinbaseadvanced": _coinbase_fetch_currencies_direct,
    "bitrue": _bitrue_fetch_currencies_direct,
    "hashkey": _hashkey_fetch_currencies_direct,
    "phemex": _phemex_fetch_currencies_direct,
    "bitstamp": _bitstamp_fetch_currencies_direct,
    "cryptocom": _cryptocom_fetch_currencies_direct,
    "okx": _okx_fetch_currencies_direct,
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
        norm_id = _norm(exch_id)
        direct = _DIRECT_FETCHERS.get(norm_id)
        # If ccxt claims no fetchCurrencies AND we have no direct fetcher, quit.
        if not inst.has.get("fetchCurrencies") and direct is None:
            log.debug("[%s] has.fetchCurrencies=False and no direct — skipping",
                       exch_id)
            return
        currs = None
        # Direct REST path for exchanges with known rate-limit problems OR
        # for those ccxt refuses to fetch currencies for (cryptocom).
        if direct is not None:
            currs = await direct(inst, self.proxies)
        if currs is None and inst.has.get("fetchCurrencies"):
            # Fallback to ccxt's built-in fetch_currencies (also tolerates rate
            # limits with retries + proxy rotation).
            try:
                currs = await self._ccxt_fetch_with_retries(inst, norm_id)
            except Exception as e:
                log.warning("[%s] fetch_currencies err: %s", exch_id, str(e)[:120])
                return
        if not currs:
            log.warning("[%s] fetch_currencies returned empty (0 currencies)",
                        exch_id)
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
        with_contract = sum(
            1 for _, r in result.items()
            for _, nd in r.get("networks", {}).items()
            if (nd.get("contract") or "").strip()
        )
        log.info("[%s] currencies refreshed: %d assets (%d named, %d net-refs w/contract)",
                 norm_id, len(result), len(names), with_contract)

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
