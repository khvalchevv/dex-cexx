"""Telegram alerter — broadcast model, clean formatting, action buttons,
ticker-collision filter via contract-address -> CoinGecko cg_id matching.
"""
import asyncio
import html
import json
import logging
import os
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
)

from blacklist import TokenBlacklist

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
SUBSCRIBERS_FILE = os.path.join(_HERE, "subscribers.json")


# ---------- chain code normalization (CEX network code -> DexScreener chain slug) ----------

# Many CEXes return non-standard network codes for the same chain. Map them all
# to DexScreener's canonical slug so we can cross-lookup contract addresses
# against TokenResolver's index.
_NETWORK_TO_DEX_CHAIN = {
    # Ethereum mainnet
    "ETH": "ethereum", "ETHEREUM": "ethereum", "ERC20": "ethereum", "ERC-20": "ethereum",
    # BSC
    "BSC": "bsc", "BEP20": "bsc", "BEP-20": "bsc", "BNB": "bsc", "BINANCE": "bsc",
    "BINANCE-SMART-CHAIN": "bsc", "BNBSMARTCHAIN": "bsc",
    # Polygon
    "POLYGON": "polygon", "MATIC": "polygon", "POLYGONPOS": "polygon",
    # Solana
    "SOL": "solana", "SOLANA": "solana", "SPL": "solana",
    # Tron
    "TRX": "tron", "TRON": "tron", "TRC20": "tron", "TRC-20": "tron",
    # Arbitrum
    "ARB": "arbitrum", "ARBITRUM": "arbitrum", "ARBONE": "arbitrum",
    "ARBITRUMONE": "arbitrum", "ARBITRUMNOVA": "arbitrum",
    # Optimism
    "OP": "optimism", "OPTIMISM": "optimism", "OPTIMISMETHEREUM": "optimism",
    # Base
    "BASE": "base", "BASECHAIN": "base", "BASEMAINNET": "base",
    # Avalanche
    "AVAX": "avalanche", "AVAXC": "avalanche", "AVALANCHE": "avalanche",
    "AVALANCHE-C": "avalanche", "CCHAIN": "avalanche",
    # Fantom, Cronos, Linea, Scroll, zkSync, Blast, Mantle, Celo, Moonbeam, Metis
    "FTM": "fantom", "FANTOM": "fantom",
    "CRO": "cronos", "CRONOS": "cronos",
    "LINEA": "linea",
    "SCROLL": "scroll",
    "ZKS": "zksync", "ZKSYNC": "zksync", "ZKSYNCERA": "zksync",
    "BLAST": "blast",
    "MNT": "mantle", "MANTLE": "mantle",
    "CELO": "celo",
    "GLMR": "moonbeam", "MOONBEAM": "moonbeam",
    "METIS": "metis",
    # L1s / non-EVM
    "SUI": "sui",
    "APT": "aptos", "APTOS": "aptos",
    "TON": "ton",
    "NEAR": "near",
    "HYPERLIQUID": "hyperliquid", "HYPE": "hyperliquid",
}


_NET_NORMALIZE_RE = None


def _norm_net_key(net_code: str) -> str:
    """Strip parens-content, spaces, underscores, dashes, dots — keep A-Z0-9."""
    global _NET_NORMALIZE_RE
    if _NET_NORMALIZE_RE is None:
        import re
        _NET_NORMALIZE_RE = (re.compile(r"\(.*?\)"), re.compile(r"[^A-Z0-9]"))
    paren_re, junk_re = _NET_NORMALIZE_RE
    k = paren_re.sub("", net_code or "").upper()
    return junk_re.sub("", k)


def cex_net_to_dex_chain(net_code: str) -> str | None:
    if not net_code:
        return None
    return _NETWORK_TO_DEX_CHAIN.get(_norm_net_key(net_code))


def cex_cg_ids_for_asset(currency_info: dict | None, resolver) -> set[str]:
    """Given currency cache entry for (exchange, asset), return the set of
    CoinGecko coin IDs that this listing matches via contract addresses."""
    if not currency_info or resolver is None:
        return set()
    out: set[str] = set()
    for net, ndata in (currency_info.get("networks") or {}).items():
        contract = (ndata or {}).get("contract")
        if not contract:
            continue
        dex_chain = cex_net_to_dex_chain(net)
        if not dex_chain:
            continue
        cg_id = resolver.cg_id_for_contract(dex_chain, contract)
        if cg_id:
            out.add(cg_id)
    return out


# ---------- formatting helpers ----------

def _short_addr(addr: str) -> str:
    if not addr or len(addr) < 14:
        return addr or ""
    return f"{addr[:8]}...{addr[-6:]}"


def _fmt_usd(x: float) -> str:
    if x >= 1_000_000:
        return f"${x/1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}k"
    return f"${x:.2f}"


def _fmt_price(x: float) -> str:
    if x >= 1:
        return f"${x:,.4f}"
    if x >= 0.01:
        return f"${x:.5f}"
    return f"${x:.8f}"


def _fmt_networks_compact(info: dict | None) -> str:
    if not info:
        return ""
    nets = info.get("networks") or {}
    if not nets:
        return ""
    parts = []
    for net, n in sorted(nets.items()):
        d = "✅" if n.get("deposit") else "❌"
        w = "✅" if n.get("withdraw") else "❌"
        parts.append(f"{net} {d}{w}")
    return " · ".join(parts[:5]) + (f" · +{len(parts)-5}" if len(parts) > 5 else "")


# ---------- URL builders ----------

_CEX_URLS = {
    "binance":          "https://www.binance.com/en/trade/{base}_{quote}",
    "bybit":            "https://www.bybit.com/en/trade/spot/{base}/{quote}",
    "okx":              "https://www.okx.com/trade-spot/{base_l}-{quote_l}",
    "kucoin":           "https://www.kucoin.com/trade/{base}-{quote}",
    "mexc":             "https://www.mexc.com/exchange/{base}_{quote}",
    "gateio":           "https://www.gate.io/trade/{base}_{quote}",
    "bitget":           "https://www.bitget.com/spot/{base}{quote}",
    "htx":              "https://www.htx.com/en-us/trade/{base_l}_{quote_l}",
    "bitmart":          "https://www.bitmart.com/trade/en?symbol={base}_{quote}",
    "kraken":           "https://pro.kraken.com/app/trade/{base_l}-{quote_l}",
    "coinbaseadvanced": "https://exchange.coinbase.com/trade/{base}-{quote}",
    "cryptocom":        "https://crypto.com/exchange/trade/{base}_{quote}",
    "whitebit":         "https://whitebit.com/trade/{base}_{quote}",
    "coinex":           "https://www.coinex.com/exchange/{base_l}-{quote_l}",
    "xt":               "https://www.xt.com/en/trade/{base_l}_{quote_l}",
    "phemex":           "https://phemex.com/spot/trade/{base}{quote}",
    "bingx":            "https://bingx.com/en/spot/{base}{quote}/",
    "hitbtc":           "https://hitbtc.com/{base}-to-{quote}",
    "poloniex":         "https://poloniex.com/spot/{base}_{quote}",
    "exmo":             "https://exmo.com/trade/{base}_{quote}",
    "upbit":            "https://upbit.com/exchange?code=CRIX.UPBIT.{quote}-{base}",
    "bithumb":          "https://www.bithumb.com/react/trade/order/{base}-{quote}",
    "p2b":              "https://p2pb2b.com/trade/{base}_{quote}",
    "bitvavo":          "https://bitvavo.com/en/trade/{base}-{quote}",
    "bitfinex":         "https://trading.bitfinex.com/t/{base}:{quote}",
    "bitstamp":         "https://www.bitstamp.net/markets/{base_l}/{quote_l}/",
    "bitrue":           "https://www.bitrue.com/trade/{base_l}_{quote_l}",
    "hashkey":          "https://global.hashkey.com/en-US/exchange/trade/spot/{base}-{quote}",
    "ascendex":         "https://ascendex.com/en/cashtrade-spottrading/{quote_l}/{base_l}",
    "lbank":            "https://www.lbank.com/trade/{base_l}_{quote_l}",
}

_EXPLORER_URLS = {
    "ethereum":   "https://etherscan.io/token/{addr}",
    "bsc":        "https://bscscan.com/token/{addr}",
    "polygon":    "https://polygonscan.com/token/{addr}",
    "arbitrum":   "https://arbiscan.io/token/{addr}",
    "optimism":   "https://optimistic.etherscan.io/token/{addr}",
    "base":       "https://basescan.org/token/{addr}",
    "avalanche":  "https://snowtrace.io/token/{addr}",
    "fantom":     "https://ftmscan.com/token/{addr}",
    "solana":     "https://solscan.io/token/{addr}",
    "tron":       "https://tronscan.org/#/token20/{addr}",
    "linea":      "https://lineascan.build/token/{addr}",
    "scroll":     "https://scrollscan.com/token/{addr}",
    "zksync":     "https://explorer.zksync.io/address/{addr}",
    "blast":      "https://blastscan.io/token/{addr}",
    "mantle":     "https://mantlescan.xyz/token/{addr}",
    "celo":       "https://celoscan.io/token/{addr}",
    "sui":        "https://suiscan.xyz/mainnet/coin/{addr}",
    "aptos":      "https://aptoscan.com/coin/{addr}",
    "ton":        "https://tonviewer.com/{addr}",
    "near":       "https://nearblocks.io/token/{addr}",
}


def cex_url(exch_id: str, pair: str) -> str | None:
    if "/" not in pair:
        return None
    base, _, quote = pair.partition("/")
    tmpl = _CEX_URLS.get(exch_id)
    if not tmpl:
        return None
    return tmpl.format(base=base, quote=quote, base_l=base.lower(), quote_l=quote.lower())


def dex_url(dex_entry: dict) -> str | None:
    chain = dex_entry.get("chain")
    pair_addr = dex_entry.get("pair_addr")
    if chain and pair_addr:
        return f"https://dexscreener.com/{chain}/{pair_addr}"
    contract = dex_entry.get("contract")
    if chain and contract:
        return f"https://dexscreener.com/{chain}/{contract}"
    return None


def explorer_url(chain: str, contract: str) -> str | None:
    if not chain or not contract:
        return None
    tmpl = _EXPLORER_URLS.get(chain.lower())
    if not tmpl:
        return None
    return tmpl.format(addr=contract)


# ---------- alert formatter (clean, no inline links — links go on buttons) ----------

def _direction_emoji(direction: str) -> str:
    return {"DEX->CEX": "🟢", "CEX->DEX": "🔵", "CEX->CEX": "🔄"}.get(direction, "🟢")


def _quote_of(pair: str) -> str:
    return pair.partition("/")[2] if "/" in pair else ""


def _short_contract(addr: str) -> str:
    if not addr or len(addr) < 14:
        return addr or ""
    return f"{addr[:7]}...{addr[-4:]}"


def _pad(s: str, w: int) -> str:
    """Truncate-and-pad to exact width."""
    s = s[:w]
    return s + " " * (w - len(s))


def _fmt_liq(x: float) -> str:
    if x >= 1_000_000:
        return f"${x/1_000_000:.1f}M"
    if x >= 1_000:
        return f"${x/1_000:.0f}k"
    return f"${x:.0f}"


def _dw(deposit, withdraw) -> str:
    """Deposit/withdraw status pair: ✅ open, ❌ closed, ❓ unknown."""
    e = lambda b: "✅" if b is True else ("❌" if b is False else "❓")
    return e(deposit) + e(withdraw)


def _agg_dw(nets: dict):
    """Aggregate deposit/withdraw across an exchange's networks (any-open)."""
    if not nets:
        return None, None
    dep = True if any(n.get("deposit") for n in nets.values()) else (
        False if any(n.get("deposit") is False for n in nets.values()) else None)
    wd = True if any(n.get("withdraw") for n in nets.values()) else (
        False if any(n.get("withdraw") is False for n in nets.values()) else None)
    return dep, wd


# Canonical network labels — exchanges call the same chain differently.
# We show a single human-readable name so the alert isn't 5 spellings of BSC.
_NET_LABELS = {
    "bep20": "BSC", "bep2": "BSC", "bsc": "BSC", "bnb": "BSC",
    "bnbsmartchain": "BSC", "bnbsmart": "BSC", "bnbchain": "BSC",
    "erc20": "ETH", "eth": "ETH", "ethereum": "ETH",
    "trc20": "TRON", "trc10": "TRON", "tron": "TRON",
    "sol": "SOL", "solana": "SOL",
    "polygon": "POLYGON", "matic": "POLYGON",
    "arb": "ARBITRUM", "arbitrum": "ARBITRUM", "arbevm": "ARBITRUM",
    "arbeth": "ARBITRUM", "arbitrumone": "ARBITRUM",
    "op": "OPTIMISM", "opt": "OPTIMISM", "optimism": "OPTIMISM",
    "avax": "AVAX", "avaxc": "AVAX", "avaxcchain": "AVAX",
    "avalanche": "AVAX", "cchain": "AVAX",
    "base": "BASE", "baseeth": "BASE",
    "ton": "TON", "openton": "TON",
    "sui": "SUI", "aptos": "APTOS", "near": "NEAR",
    "xrpl": "XRP", "xrp": "XRP",
    "cardano": "ADA", "ada": "ADA",
    "algo": "ALGO", "algorand": "ALGO",
    "atom": "COSMOS", "cosmos": "COSMOS",
    "osmo": "OSMOSIS", "osmosis": "OSMOSIS",
    "sei": "SEI", "seiv2": "SEI", "seievm": "SEI",
    "dot": "POLKADOT", "polkadot": "POLKADOT",
    "ksm": "KUSAMA", "kusama": "KUSAMA",
    "zksync": "ZKSYNC", "zksyncera": "ZKSYNC",
    "linea": "LINEA", "scroll": "SCROLL", "mantle": "MANTLE",
    "blast": "BLAST", "berachain": "BERA", "bera": "BERA",
    "hyperevm": "HYPER", "hyperliquid": "HYPER",
    "unichain": "UNICHAIN", "taiko": "TAIKO", "monad": "MONAD",
    "katana": "KATANA", "worldchain": "WORLD", "world": "WORLD",
    "fantom": "FTM", "ftm": "FTM", "sonic": "FTM",
    "cronos": "CRONOS", "cro": "CRONOS", "cronoszkevm": "CRONOS",
    "celo": "CELO", "klaytn": "KLAY", "kaia": "KLAY",
    "kcc": "KCC", "ronin": "RONIN",
    "xdc": "XDC", "merlin": "MERLIN",
    "oktc": "OKTC", "okc": "OKTC", "okexchain": "OKTC",
    "manta": "MANTA", "kava": "KAVA", "kavaevm": "KAVA",
    "injective": "INJ", "inj": "INJ",
    "btc": "BTC", "bitcoin": "BTC",
    "ltc": "LTC", "litecoin": "LTC",
    "doge": "DOGE", "dogecoin": "DOGE",
    "bch": "BCH", "bchsv": "BCH", "bsv": "BSV",
    "etc": "ETC", "eos": "EOS",
    "xtz": "TEZOS", "tezos": "TEZOS",
    "xlm": "STELLAR", "stellar": "STELLAR",
    "xmr": "MONERO", "monero": "MONERO",
    "hbar": "HBAR", "hedera": "HBAR",
    "vet": "VET", "vechain": "VET",
    "zil": "ZIL", "zilliqa": "ZIL",
    "iota": "IOTA", "miota": "IOTA",
    "waves": "WAVES", "neo": "NEO", "ont": "ONT",
    "icp": "ICP", "internetcomputer": "ICP",
    "fil": "FIL", "filecoin": "FIL",
    "iotx": "IOTEX", "iotex": "IOTEX",
    "kda": "KADENA", "kadena": "KADENA",
    "stx": "STACKS", "stacks": "STACKS",
    "egld": "MULTIVERSX", "multiversx": "MULTIVERSX",
}


def _pretty_net(net: str) -> str:
    """Canonicalise a network name for display (bep20 → BSC, erc20 → ETH, …)."""
    key = "".join(ch for ch in (net or "").lower() if ch.isalnum())
    return _NET_LABELS.get(key, (net or "").upper())


def _cex_block(eid: str, price_str: str, nets: dict, net_limit: int = 6,
               pair: str | None = None) -> list[str]:
    """Tree-style CEX rendering:
        EXCHANGE  $price
          └ BSC: 🟢🟢                       (per-network deposit/withdraw)
    Withdraw-open networks first so the usable ones survive truncation.
    Exchange name is hyperlinked to its trading page when `pair` is given.
    Network labels are canonicalised (bep20→BSC, erc20→ETH, etc)."""
    price_part = f" {price_str}" if price_str else ""
    url = cex_url(eid, pair) if pair else None
    name_html = (f'<a href="{url}"><b>{eid.upper()}</b></a>' if url
                 else f"<b>{eid.upper()}</b>")
    lines = [f"{name_html}{price_part}"]
    items = sorted(nets.items(),
                   key=lambda kv: (kv[1].get("withdraw") is not True,
                                   kv[1].get("deposit") is not True, kv[0]))
    for net, nd in items[:net_limit]:
        lines.append(f"  └ {_pretty_net(net)}: {_dw(nd.get('deposit'), nd.get('withdraw'))}")
    if len(items) > net_limit:
        lines.append(f"  └ +{len(items) - net_limit} more")
    return lines


def _venue_label(v: dict) -> str:
    if v["kind"] == "dex":
        return f"{(v.get('dex') or 'dex')}_{v.get('chain') or ''}"
    return v["eid"].upper()


_OKX_URL_SLUG = {
    "ethereum": "ethereum", "bsc": "bsc", "solana": "solana", "base": "base",
    "arbitrum": "arbitrum-one", "polygon": "polygon", "optimism": "optimism",
    "avalanche": "avalanche", "tron": "tron", "sui": "sui", "ton": "ton",
    "aptos": "aptos", "zksync": "zksync", "linea": "linea", "scroll": "scroll",
    "mantle": "mantle", "blast": "blast", "sei": "sei", "cronos": "cronos",
    "fantom": "sonic", "celo": "celo", "berachain": "berachain",
}


def okx_token_url(chain: str, contract: str) -> str | None:
    slug = _OKX_URL_SLUG.get(chain)
    return f"https://web3.okx.com/token/{slug}/{contract}" if slug and contract else None


def _venue_price(v: dict, side: str) -> float:
    return v["buy"] if side == "buy" else v["sell"]


def _leg_lines(label: str, v: dict, side: str, nets_by_eid: dict) -> list[str]:
    """Render a Buy/Sell leg. DEX: one line + contract. CEX: tree block."""
    price = _fmt_price(_venue_price(v, side))
    if v["kind"] == "dex":
        chain = (v.get("chain") or "dex").upper()
        liq = _fmt_liq(v.get("liq", 0))
        du = dex_url(v)
        chain_html = f'<a href="{du}">{chain}</a>' if du else chain
        out = [f"<b>{label}:</b> {chain_html} (DEX)  {price}  (liq {liq})"]
        if v.get("contract"):
            out.append(f"<code>{v['contract']}</code>")
        return out
    blk = _cex_block(v["eid"], price, nets_by_eid.get(v["eid"]) or {},
                     pair=v.get("pair"))
    blk[0] = f"<b>{label}:</b> " + blk[0]
    return blk


def _format_new_group(gid: str, group: dict, okx_pools: dict,
                       ds_pools: dict) -> str:
    """Structured new-token TG message:

        <b>TICKER</b>
        <b>- GATE</b>: symbol
        <code>contract</code>
        networks: bep20, erc20
        <b>- bingx</b>: symbol
        <code>contract</code>
        networks: eth

        <b>Прив'язаний декс пул:</b>
        - <b>BSC</b>
        <a href="pool_url">dexscreener.com/bsc/…</a>
        - <b>SOLANA</b>
        <a href="pool_url">web3.okx.com/…</a>
    """
    P: list[str] = []
    display = group.get("display") or gid
    ticker = gid.partition("#")[0]
    P.append(f"<b>{html.escape(ticker)}</b>  {html.escape(display)}")
    # CEX listings (one block per exchange)
    for lst in group.get("listings", []):
        eid = lst.get("eid", "")
        sym = lst.get("symbol", "")
        nets = lst.get("networks") or {}
        # dedup contracts across networks (many chains share the same evm addr)
        contracts_seen: set[str] = set()
        contracts: list[str] = []
        for n, nd in nets.items():
            c = (nd.get("contract") or "").strip()
            if c and c.lower() not in contracts_seen:
                contracts_seen.add(c.lower())
                contracts.append(c)
        net_names = sorted(nets.keys())
        P.append(f"<b>- {html.escape(eid.upper())}</b>: {html.escape(sym)}")
        for c in contracts:
            P.append(f"<code>{html.escape(c)}</code>")
        if net_names:
            P.append("networks: " + ", ".join(html.escape(n) for n in net_names))
    # DEX pools (per chain)
    pool_lines: list[str] = []
    seen_chains: set[str] = set()
    for chain_key, entry in {**okx_pools, **ds_pools}.items():
        ch = (entry.get("chain") or chain_key.partition(":")[0] or "").upper()
        if not ch or ch in seen_chains:
            continue
        seen_chains.add(ch)
        u = dex_url(entry)
        if not u:
            continue
        pool_lines.append(f"- <b>{html.escape(ch)}</b>")
        pool_lines.append(f'<a href="{u}">{html.escape(u)}</a>')
    if pool_lines:
        P.append("")
        P.append("<b>Прив'язаний декс пул:</b>")
        P.extend(pool_lines)
    return "\n".join(P)


async def format_alert(alert: dict, currency_cache=None) -> tuple[str, InlineKeyboardMarkup]:
    """Clean per-token alert: header, Route (buy->sell), the two legs with
    price + deposit/withdraw (CEX) / vol + contract (DEX), an `other CEX`
    block listing every other exchange with its price and D/W status, then
    contracts and links."""
    from datetime import datetime, timezone
    gid = alert["group"]
    display = alert.get("display") or gid
    spread = alert["spread"]
    buy = alert["buy"]
    sell = alert["sell"]
    cex = alert.get("cex") or []           # [{eid,symbol,pair,bid,ask}]
    dex = alert.get("dex") or []           # [{chain,dex,priceUsd,vol24h,contract,...}]
    listings = alert.get("listings") or []
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    nets_by_eid: dict[str, dict] = {}
    for l in listings:
        nets_by_eid.setdefault(l["eid"], {}).update(l.get("networks") or {})

    def vlabel(v):
        return (f"{(v.get('chain') or 'DEX').upper()} (DEX)" if v["kind"] == "dex"
                else v["eid"].upper())

    P = []
    ticker = alert.get("ticker") or gid.partition("#")[0]
    P.append(f"‼️ <b>{ticker}</b> / +{spread:.2f}%")
    P.append(f"<b>Route:</b> {vlabel(buy)} → {vlabel(sell)}")
    P.append("")
    P.extend(_leg_lines("Buy", buy, "buy", nets_by_eid))
    P.extend(_leg_lines("Sell", sell, "sell", nets_by_eid))

    # other CEX — every CEX listing except the legs. First the ones with a
    # live price (from cex_book), then the universe-listed exchanges we know
    # support the token but currently have no ticker in the book (rendered
    # without a price line).
    legs = {v["eid"] for v in (buy, sell) if v["kind"] == "cex"}
    others = [q for q in cex if q["eid"] not in legs]
    priced_eids = legs | {q["eid"] for q in others}
    # exchanges from universe listings that aren't a leg and have no live price
    silent = sorted(e for e in nets_by_eid if e not in priced_eids)
    if others or silent:
        P.append("")
        P.append("<b>other CEX:</b>")
        for q in sorted(others, key=lambda x: x["ask"])[:12]:
            nets = nets_by_eid.get(q["eid"]) or {}
            P.extend(_cex_block(q["eid"], _fmt_price(q["ask"]), nets,
                                 net_limit=5, pair=q.get("pair")))
        for eid in silent[:12]:
            nets = nets_by_eid.get(eid) or {}
            # no price, no clickable pair — show name + networks only
            P.extend(_cex_block(eid, "", nets, net_limit=5))

    # other DEX chains (besides the leg) — price + vol
    leg_chains = {v.get("chain") for v in (buy, sell) if v["kind"] == "dex"}
    odex = [e for e in dex if e.get("chain") not in leg_chains]
    if odex:
        P.append("")
        P.append("<b>other DEX:</b>")
        for e in sorted(odex, key=lambda x: -(x.get("liquidity") or 0))[:8]:
            chain = (e.get('chain') or '').upper()
            du = dex_url(e)
            chain_html = f'<a href="{du}">{chain}</a>' if du else chain
            P.append(f"  {chain_html}: {_fmt_price(e['priceUsd'])}"
                     f"  (liq {_fmt_liq(e.get('liquidity', 0))})")

    # Contracts (tap-to-copy)
    contracts, seen = [], set()
    for e in dex:
        c = e.get("contract") or ""
        if c and c.lower() not in seen:
            seen.add(c.lower())
            contracts.append(f"{(e.get('chain') or '').upper()}: <code>{c}</code>")
    for l in listings:
        for ch, nd in (l.get("networks") or {}).items():
            c = nd.get("contract") or ""
            if c and c.lower() not in seen and len(contracts) < 10:
                seen.add(c.lower())
                contracts.append(f"{ch.upper()}: <code>{c}</code>")
    if contracts:
        P.append("")
        P.append("<b>Contracts:</b>")
        P.extend(contracts)

    P.append("")
    P.append(display)
    P.append(ts)
    body = "\n".join(P)

    # ----- buttons: ONLY the buy and sell legs (buy first), then blacklist -----
    def _leg_url(leg):
        if leg["kind"] == "cex":
            return cex_url(leg["eid"], leg.get("pair", ""))
        return okx_token_url(leg.get("chain"), leg.get("contract")) or dex_url(leg)

    def _leg_btn_label(prefix, leg):
        venue = (leg["eid"].upper() if leg["kind"] == "cex"
                 else f"{(leg.get('chain') or 'DEX').upper()} (DEX)")
        return f"{prefix} {venue}"

    action: list[InlineKeyboardButton] = []
    for prefix, leg in (("🟢 Buy", buy), ("🔴 Sell", sell)):   # buy first, then sell
        u = _leg_url(leg)
        if u:
            action.append(InlineKeyboardButton(_leg_btn_label(prefix, leg), url=u))
    rows = [action] if action else []
    # info deep-link — opens @researcheer_bot pre-filled with /start <TICKER>;
    # researcher strips the '$' and treats the arg as a token query.
    base_ticker = gid.partition("#")[0]
    rows.append([InlineKeyboardButton("Info",
        url=f"https://t.me/researcheer_bot?start={base_ticker}")])
    rows.append([InlineKeyboardButton(f"🚫 Blacklist {display[:24]}",
                                      callback_data=f"bl:{base_ticker}")])
    return body, InlineKeyboardMarkup(rows)


# ---------- subscribers ----------

class SubscriberStore:
    def __init__(self, path: str = SUBSCRIBERS_FILE):
        self.path = path
        self.chat_ids: set[int] = set()
        self.lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.chat_ids = set(int(x) for x in data.get("chat_ids", []))
            log.info("subscribers loaded: %d chat_id(s)", len(self.chat_ids))
        except FileNotFoundError:
            self.chat_ids = set()

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"chat_ids": sorted(self.chat_ids)}, f)
        except Exception as e:
            log.warning("subscribers save err: %s", e)

    async def add(self, chat_id: int) -> bool:
        async with self.lock:
            if chat_id in self.chat_ids:
                return False
            self.chat_ids.add(chat_id)
            self._save()
            return True

    async def remove(self, chat_id: int) -> bool:
        async with self.lock:
            if chat_id not in self.chat_ids:
                return False
            self.chat_ids.discard(chat_id)
            self._save()
            return True

    async def snapshot(self) -> list[int]:
        async with self.lock:
            return list(self.chat_ids)


# ---------- alerter ----------

class TelegramAlerter:
    def __init__(self, currency_cache=None, blacklist: TokenBlacklist | None = None,
                  resolver=None, cex_book=None, dex_book=None, ds_book=None,
                  universe=None, exchanges=None):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.currency_cache = currency_cache
        self.subs = SubscriberStore()
        self.blacklist = blacklist or TokenBlacklist()
        self.resolver = resolver
        self.cex_book = cex_book
        self.dex_book = dex_book
        self.ds_book = ds_book
        self.universe = universe
        self.exchanges = exchanges or {}
        self.app: Application | None = None
        if not self.token:
            log.warning("TELEGRAM_BOT_TOKEN missing — alerts will be logged only")
            return
        from telegram.ext import MessageHandler, filters
        # Default read/write timeouts (5s) are too tight when TG API is slow;
        # snapshot replies (13-line messages via sendMessage) sporadically
        # timed out at ~5s. 20s comfortably fits any real reply.
        self.app = (Application.builder()
                    .token(self.token)
                    .read_timeout(20)
                    .write_timeout(20)
                    .connect_timeout(10)
                    .build())
        self.app.add_handler(CommandHandler("start", self._cmd_start))
        self.app.add_handler(CommandHandler("stop", self._cmd_stop))
        self.app.add_handler(CommandHandler("status", self._cmd_status))
        self.app.add_handler(CommandHandler("blacklist", self._cmd_blacklist))
        self.app.add_handler(CommandHandler("unblacklist", self._cmd_unblacklist))
        self.app.add_handler(CommandHandler("check", self._cmd_check))
        self.app.add_handler(CommandHandler("c", self._cmd_check_contract))
        self.app.add_handler(MessageHandler(
            filters.Regex(r"^\$[A-Za-z0-9][A-Za-z0-9_]*\s*$"),
            self._cmd_check_dollar))
        # debug catch-all — logs every message so we can see whether TG delivers
        self.app.add_handler(MessageHandler(filters.ALL, self._cmd_debug), group=99)
        self.app.add_handler(CallbackQueryHandler(self._cb_blacklist, pattern=r"^bl:"))

    async def _cmd_debug(self, update, context):
        try:
            msg = update.message
            if msg is None:
                return
            log.info("TG rcvd: chat=%s text=%r", update.effective_chat.id, (msg.text or msg.caption or "")[:60])
        except Exception:
            pass
        self.app.add_error_handler(self._on_error)

    async def _on_error(self, update, context):
        """Silently log any unhandled exception in a handler (TG timeouts, etc.)
        so the update processor keeps running clean."""
        log.warning("TG handler err: %s", str(context.error)[:160])

    async def start(self) -> None:
        if not self.app:
            return
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=True)
        me = await self.app.bot.get_me()
        log.info("TG bot started: @%s (subs=%d, blacklist=%d)",
                 me.username, len(await self.subs.snapshot()),
                 len(await self.blacklist.snapshot()))

    async def stop(self) -> None:
        if not self.app:
            return
        try:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()
        except Exception as e:
            log.warning("TG stop err: %s", e)

    async def _cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        added = await self.subs.add(chat.id)
        msg = (f"✅ Subscribed.\n"
               f"Alerts when spread ≥ {os.getenv('MIN_SPREAD_PCT','6.0')}%.\n"
               f"/stop · /status · /blacklist · /unblacklist TOKEN"
               if added else "Already subscribed.")
        await update.message.reply_text(msg)
        if added:
            log.info("new subscriber: chat_id=%s", chat.id)

    async def _cmd_stop(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        chat = update.effective_chat
        removed = await self.subs.remove(chat.id)
        await update.message.reply_text("Unsubscribed." if removed else "You were not subscribed.")

    async def _cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        subs = await self.subs.snapshot()
        bl = await self.blacklist.snapshot()
        await update.message.reply_text(
            f"Subscribers: {len(subs)}\n"
            f"Min spread: {os.getenv('MIN_SPREAD_PCT','6.0')}%\n"
            f"Scan interval: {os.getenv('SCAN_INTERVAL_SEC','5')}s\n"
            f"Cooldown: {os.getenv('ALERT_COOLDOWN_SEC','300')}s\n"
            f"Blacklist ({len(bl)}): " + (", ".join(sorted(bl)) or "(empty)")
        )

    async def _cmd_blacklist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        bl = await self.blacklist.snapshot()
        text = ("Blacklist:\n" + "\n".join(sorted(bl))) if bl else "Blacklist is empty."
        await update.message.reply_text(text)

    async def _cmd_unblacklist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        args = context.args or []
        if not args:
            await update.message.reply_text("Usage: /unblacklist TOKEN")
            return
        removed = []
        for t in args:
            if await self.blacklist.remove(t):
                removed.append(t.upper())
        await update.message.reply_text(
            f"Removed: {', '.join(removed)}" if removed else "Nothing matched."
        )

    async def _cmd_check(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """`/check TOKEN` — dump, per exchange, the networks + deposit/withdraw
        + contract this token exposes. Pure data view (no CG)."""
        args = context.args or []
        if not args:
            await update.message.reply_text("Usage: /check TOKEN\nExample: /check PEPE")
            return
        token = args[0].upper()
        lines = [f"<b>🔍 {token}</b>", ""]

        cache = self.currency_cache
        if cache is None:
            await update.message.reply_text("(no currency cache)")
            return
        async with cache.lock:
            exchanges = sorted(cache.cache.keys())

        per_exchange_blocks: list[str] = []
        no_data_exchanges: list[str] = []
        for eid in exchanges:
            info = await cache.get(eid, token)
            if not info:
                no_data_exchanges.append(eid)
                continue
            nets = info.get("networks") or {}
            if not nets:
                continue
            net_lines = []
            for n, nd in sorted(nets.items()):
                contract = nd.get("contract") or ""
                d = "✅" if nd.get("deposit") else "❌"
                w = "✅" if nd.get("withdraw") else "❌"
                short_contract = (contract[:10] + "…" + contract[-6:]
                                   if len(contract) > 18 else (contract or "—"))
                net_lines.append(f"  <code>{n}</code> {d}{w} {short_contract}")
            per_exchange_blocks.append(f"<b>{eid}</b>\n" + "\n".join(net_lines))

        lines.append(f"<b>{len(per_exchange_blocks)} exchanges have it</b>")
        if no_data_exchanges:
            lines.append(f"<i>no '{token}': {', '.join(no_data_exchanges)}</i>")
        lines.append("")
        lines.extend(per_exchange_blocks)

        text = "\n".join(lines)
        # Telegram caps at 4096 chars per message; split if needed.
        chunks = []
        cur = ""
        for line in text.split("\n"):
            if len(cur) + len(line) + 1 > 3800:
                chunks.append(cur)
                cur = line
            else:
                cur = (cur + "\n" + line) if cur else line
        if cur:
            chunks.append(cur)
        for chunk in chunks:
            await update.message.reply_text(chunk, parse_mode=ParseMode.HTML,
                                              disable_web_page_preview=True)

    # ---------- checker: /c <contract>  or  "$TICKER" ----------

    async def _safe_reply(self, update, text: str, **kwargs) -> None:
        """Wrap reply_text so TG timeouts don't crash the handler."""
        try:
            await update.message.reply_text(text, **kwargs)
        except Exception as e:
            log.warning("TG reply err: %s", str(e)[:120])

    async def _cmd_check_contract(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """/c <contract>  — full snapshot of the group that owns this contract:
        CEX prices + deposit/withdraw per network + DEX pools (OKX + DS)."""
        args = context.args or []
        if not args:
            await self._safe_reply(update,
                "Usage: /c <contract_address>\nExample: /c 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
            return
        contract = args[0].strip().lower()
        gid = self._find_gid_by_contract(contract)
        if not gid:
            await self._safe_reply(update,
                f"❌ no group in universe matches contract <code>{contract}</code>",
                parse_mode=ParseMode.HTML)
            return
        await self._send_check_snapshot(update, gid)

    async def _cmd_check_dollar(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """`$TICKER` (bare message, no slash) — same as /c but by ticker."""
        import re as _re
        raw = (update.message.text or "").strip()
        m = _re.match(r"^\$([A-Za-z0-9][A-Za-z0-9_]*)", raw)
        if not m:
            log.info("$-handler: no match for %r", raw[:40])
            return
        ticker = m.group(1).upper()
        log.info("$-handler: chat=%s ticker=%s", update.effective_chat.id, ticker)
        gid = self._find_gid_by_ticker(ticker)
        if not gid:
            await self._safe_reply(update,
                f"❌ no token <code>{ticker}</code> in universe",
                parse_mode=ParseMode.HTML)
            return
        await self._send_check_snapshot(update, gid)

    def _find_gid_by_contract(self, contract_lower: str) -> str | None:
        if self.universe is None:
            return None
        from dex_watcher import _norm_contract
        target = _norm_contract(contract_lower).lower()
        for gid, g in self.universe.groups.items():
            for lst in g.get("listings", []):
                for _net, nd in (lst.get("networks") or {}).items():
                    c = _norm_contract(nd.get("contract") or "").lower()
                    if c and c == target:
                        return gid
        return None

    def _find_gid_by_ticker(self, ticker: str) -> str | None:
        if self.universe is None:
            return None
        t = ticker.upper()
        if t in self.universe.groups:
            return t
        for gid in self.universe.groups:
            if gid.partition("#")[0] == t:
                return gid
        return None

    async def _send_check_snapshot(self, update, gid: str):
        text = await self._format_check_snapshot(gid)
        chunks, cur = [], ""
        for line in text.split("\n"):
            if len(cur) + len(line) + 1 > 3800:
                chunks.append(cur); cur = line
            else:
                cur = (cur + "\n" + line) if cur else line
        if cur:
            chunks.append(cur)
        base_ticker = gid.partition("#")[0]
        info_kb = InlineKeyboardMarkup([[InlineKeyboardButton(
            "Info",
            url=f"https://t.me/researcheer_bot?start={base_ticker}")]])
        for i, chunk in enumerate(chunks):
            last = (i == len(chunks) - 1)
            try:
                await update.message.reply_text(chunk, parse_mode=ParseMode.HTML,
                                                  disable_web_page_preview=True,
                                                  reply_markup=info_kb if last else None)
            except Exception as e:
                log.warning("check-snapshot reply err (%s): %s", gid, str(e)[:120])
                return

    def _get_ex(self, eid: str):
        """Return the ccxt.pro instance for eid, handling sharded ids like
        bingx#0..bingx#3 — we take any live shard."""
        ex = self.exchanges.get(eid)
        if ex is not None:
            return ex
        for k, e in self.exchanges.items():
            if k.partition("#")[0] == eid:
                return e
        return None

    async def _rest_ticker_fresh(self, eid: str, sym: str, timeout: float = 4.0):
        """REST fetch_ticker for on-demand snapshot. Uses the bot's running
        ccxt.pro instance (markets loaded, no load_markets overhead) —
        temporarily swaps its aiohttp_proxy to None so we hit exchange REST
        directly. Random proxies from the pool are too slow/dead for
        interactive TG replies (~2s timeout window)."""
        ex = self._get_ex(eid)
        if ex is None:
            log.info("check-rest %s/%s: no instance", eid, sym)
            return None
        # Pick pair candidates present in markets
        candidates = []
        for quote in ("USDT", "USDC"):
            pair = f"{sym}/{quote}"
            if pair in ex.markets:
                candidates.append(pair)
        if not candidates:
            log.info("check-rest %s/%s: no matching market", eid, sym)
            return None
        # Direct connection (no proxy) — fastest for exchanges that aren't
        # geo-blocked. Blocked ones (binance from some IPs) will error out
        # quickly and we accept the loss (they're usually WS-cached anyway).
        orig_proxy = getattr(ex, "aiohttp_proxy", None)
        ex.aiohttp_proxy = None
        try:
            for pair in candidates:
                try:
                    t = await asyncio.wait_for(ex.fetch_ticker(pair), timeout=timeout)
                    bid, ask = t.get("bid"), t.get("ask")
                    if bid and ask:
                        return (float(bid) + float(ask)) / 2, pair
                    last = t.get("last")
                    if last:
                        return float(last), pair
                except asyncio.TimeoutError:
                    log.info("check-rest %s/%s direct timeout(%ss)", eid, pair, timeout)
                except Exception as e:
                    log.info("check-rest %s/%s direct err: %s: %s",
                             eid, pair, type(e).__name__, str(e)[:50])
        finally:
            ex.aiohttp_proxy = orig_proxy
        return None

    async def _watch_ticker_once(self, eid: str, sym: str, timeout: float = 8.0):
        """On-demand WS-ticker for a single exchange."""
        ex = self._get_ex(eid)
        if ex is None:
            log.info("watch_once %s: no exchange instance", eid)
            return None
        if not getattr(ex, "markets", None):
            log.info("watch_once %s: markets not loaded", eid)
            return None
        # find any valid pair for this symbol
        for quote in ("USDT", "USDC", "USD", "BTC"):
            pair = f"{sym}/{quote}"
            if pair not in ex.markets:
                continue
            try:
                t = await asyncio.wait_for(ex.watch_ticker(pair), timeout=timeout)
                bid, ask = t.get("bid"), t.get("ask")
                if bid and ask:
                    return (float(bid) + float(ask)) / 2, pair
                last = t.get("last")
                if last:
                    return float(last), pair
                log.info("watch_once %s %s: ticker empty %r", eid, pair,
                         {k: t.get(k) for k in ("bid","ask","last","close")})
            except asyncio.TimeoutError:
                log.info("watch_once %s %s: TIMEOUT %.0fs", eid, pair, timeout)
                continue
            except Exception as e:
                log.info("watch_once %s %s: %s: %s", eid, pair,
                         type(e).__name__, str(e)[:80])
                continue
        # fallback: watch_order_book
        for quote in ("USDT", "USDC"):
            pair = f"{sym}/{quote}"
            if pair not in ex.markets:
                continue
            try:
                ob = await asyncio.wait_for(ex.watch_order_book(pair, limit=5),
                                             timeout=timeout)
                bids, asks = ob.get("bids") or [], ob.get("asks") or []
                if bids and asks and bids[0] and asks[0]:
                    return (float(bids[0][0]) + float(asks[0][0])) / 2, pair
                log.info("watch_once %s %s: OB empty", eid, pair)
            except asyncio.TimeoutError:
                log.info("watch_once %s %s: OB TIMEOUT", eid, pair)
                continue
            except Exception as e:
                log.info("watch_once %s %s OB: %s: %s", eid, pair,
                         type(e).__name__, str(e)[:80])
                continue
        # last resort: is the symbol even listed?
        avail = [p for p in ex.markets if p.startswith(f"{sym}/")]
        log.info("watch_once %s %s: no data via WS; markets_for_symbol=%s",
                 eid, sym, avail[:5])
        return None

    async def _format_check_snapshot(self, gid: str) -> str:
        """Clean checker snapshot:

            🔍 TICKER  Display Name

            EXCHANGES:
            - BINANCE  $36.28
              contract: 0xabc… (bep20)
              networks: 🟢 bep20, 🟢 erc20
            - BYBIT  $36.30
              …

            DEX pools (by liq desc):
            - BSC  $36.29  liq $1.2M
              <pool url>
            - ETHEREUM  $36.31  liq $500K
              <pool url>
        """
        from dex_watcher import _norm_net, _norm_contract
        g = self.universe.groups[gid]
        display = g.get("display", gid)
        listings = g.get("listings", [])
        base_ticker = gid.partition("#")[0]

        P: list[str] = []
        header = f"🔍 <b>{base_ticker}</b>  {display}"
        if self.blacklist.contains_sync(base_ticker):
            header += "  🚫"
        P.append(header)

        # ----- Collect per-exchange listings (dedup) + fetch live prices -----
        cex_snap = await self.cex_book.snapshot() if self.cex_book is not None else {}
        seen_eids: set[str] = set()
        eid_sym: dict[str, str] = {}
        nets_by_eid: dict[str, dict] = {}
        for lst in listings:
            eid = lst["eid"]
            nets_by_eid.setdefault(eid, {}).update(lst.get("networks") or {})
            if eid in seen_eids:
                continue
            seen_eids.add(eid)
            eid_sym[eid] = lst["symbol"].upper()
        # Merge in exchanges streaming this ticker but missing from universe
        for eid_in_book in cex_snap.get(base_ticker, {}).keys():
            if eid_in_book not in seen_eids:
                seen_eids.add(eid_in_book)
                eid_sym[eid_in_book] = base_ticker

        prices: dict[str, tuple[float, str]] = {}
        need_ws: list[tuple[str, str]] = []
        for eid, sym in eid_sym.items():
            q = (cex_snap.get(sym) or {}).get(eid)
            if q and q.get("bid") and q.get("ask"):
                prices[eid] = ((q["bid"] + q["ask"]) / 2,
                                q.get("pair", f"{sym}/USDT"))
            else:
                need_ws.append((eid, sym))
        if need_ws:
            try:
                results = await asyncio.wait_for(asyncio.gather(
                    *[self._rest_ticker_fresh(eid, sym, timeout=6)
                      for eid, sym in need_ws],
                    return_exceptions=True), timeout=7.0)
            except asyncio.TimeoutError:
                results = [None] * len(need_ws)
            for (eid, sym), res in zip(need_ws, results):
                if isinstance(res, tuple) and res[0]:
                    prices[eid] = res
        stale = sorted(e for e in eid_sym if e not in prices)

        # ----- EXCHANGES block: name + price + contract + networks -----
        P.append("")
        P.append(f"<b>EXCHANGES ({len(prices)}/{len(eid_sym)} live):</b>")
        # Sort by price asc (cheapest venue at top)
        for eid, (mid, pair) in sorted(prices.items(), key=lambda kv: kv[1][0]):
            nets = nets_by_eid.get(eid) or {}
            u = cex_url(eid, pair)
            name = (f'<a href="{u}">{eid.upper()}</a>' if u else eid.upper())
            P.append(f"- {name}  {_fmt_price(mid)}")
            # unique contracts across this exchange's networks
            seen_c: set[str] = set()
            contract_lines: list[str] = []
            for net, nd in nets.items():
                c = (nd.get("contract") or "").strip()
                if c and c.lower() not in seen_c:
                    seen_c.add(c.lower())
                    contract_lines.append(f"  <code>{c}</code>  ({net.lower()})")
            for line in contract_lines[:3]:
                P.append(line)
            if len(contract_lines) > 3:
                P.append(f"  <i>+{len(contract_lines) - 3} more contracts</i>")
            # networks with D/W badges
            if nets:
                net_labels = []
                for net, nd in sorted(nets.items()):
                    net_labels.append(
                        f"{_dw(nd.get('deposit'), nd.get('withdraw'))} {net.lower()}"
                    )
                P.append("  " + ", ".join(net_labels[:8]))
                if len(net_labels) > 8:
                    P.append(f"  <i>+{len(net_labels)-8} more networks</i>")
        if stale:
            P.append(f"  <i>no live quote: {', '.join(stale)}</i>")

        # ----- DEX pools: by chain, sorted by liquidity desc -----
        okx_snap = await self.dex_book.snapshot() if self.dex_book is not None else {}
        ds_snap = await self.ds_book.snapshot() if self.ds_book is not None else {}
        # per-chain best pool. Prefer higher liquidity. If OKX and DS both
        # cover same chain, pick the higher-liq one.
        best_by_chain: dict[str, dict] = {}
        for src, snap in (("OKX", okx_snap), ("DS", ds_snap)):
            for e in (snap.get(gid) or {}).values():
                ch = (e.get("chain") or "").upper()
                if not ch:
                    continue
                liq = float(e.get("liquidity") or 0)
                cur = best_by_chain.get(ch)
                if cur is None or liq > cur["_liq"]:
                    best_by_chain[ch] = {
                        "src": src,
                        "chain": ch,
                        "price": float(e.get("priceUsd") or 0),
                        "vol24h": float(e.get("vol24h") or 0),
                        "_liq": liq,
                        "contract": e.get("contract"),
                        "url": e.get("url"),
                    }
        P.append("")
        if best_by_chain:
            P.append(f"<b>DEX pools ({len(best_by_chain)} chains, by liq):</b>")
            ordered = sorted(best_by_chain.values(),
                              key=lambda x: -(x["_liq"] or x["vol24h"] or 0))
            for pool in ordered:
                ch = pool["chain"]
                url = pool["url"] or (okx_token_url(ch.lower(), pool["contract"])
                                       if pool["src"] == "OKX" else None)
                chain_lbl = (f'<a href="{url}">{ch}</a>' if url else ch)
                liq_s = f"liq {_fmt_liq(pool['_liq'])}" if pool["_liq"] else ""
                vol_s = f"vol {_fmt_liq(pool['vol24h'])}" if pool["vol24h"] else ""
                extras = " · ".join(x for x in (liq_s, vol_s) if x)
                extras_s = f"  · {extras}" if extras else ""
                P.append(f"- {chain_lbl} ({pool['src']})  "
                         f"{_fmt_price(pool['price'])}{extras_s}")
        else:
            P.append("<b>DEX pools:</b> — none")

        return "\n".join(P)

    async def _cb_blacklist(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        q = update.callback_query
        await q.answer()
        data = q.data or ""
        if not data.startswith("bl:"):
            return
        token = data[3:].strip()
        added = await self.blacklist.add(token)
        # Mark the original alert visually (drop the button, append a tag).
        original_html = q.message.text_html or q.message.text or ""
        suffix = f"\n\n🚫 <b>Blacklisted {token}</b>" if added else "\n\n(already blacklisted)"
        try:
            await q.edit_message_text(
                original_html + suffix,
                parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                reply_markup=None,
            )
        except Exception as e:
            log.debug("callback edit err: %s", e)
        if added:
            log.info("blacklisted %s by chat_id=%s", token, q.from_user.id)

    async def notify_new_groups(self, gids: set, groups: dict) -> None:
        """Push a structured TG message for each freshly-discovered token
        (group id present after a universe rebuild but not before)."""
        if not self.app:
            return
        chat_ids = await self.subs.snapshot()
        if not chat_ids:
            log.info("new_groups (no subs): %d gids", len(gids))
            return
        dex_snap = await self.dex_book.snapshot() if self.dex_book else {}
        ds_snap = await self.ds_book.snapshot() if self.ds_book else {}
        for gid in sorted(gids):
            g = groups.get(gid)
            if not g:
                continue
            okx_pools = dex_snap.get(gid, {})
            ds_pools = ds_snap.get(gid, {})
            # DEX watchers haven't cycled these fresh contracts yet — do an
            # on-demand DexScreener batch fetch so the very first notification
            # already has pool links.
            if not okx_pools and not ds_pools:
                ds_pools = await self._fetch_ds_inline(g)
            text = _format_new_group(gid, g, okx_pools, ds_pools)
            for cid in chat_ids:
                try:
                    await self.app.bot.send_message(
                        chat_id=cid, text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                except Exception as e:
                    log.warning("new_group TG err chat=%s gid=%s: %s",
                                cid, gid, str(e)[:80])

    async def _fetch_ds_inline(self, group: dict) -> dict:
        """One-shot DexScreener fetch for a group's contracts — used when
        the DS watcher hasn't yet indexed a freshly-listed token."""
        try:
            import aiohttp, random
            from dex_screener_watcher import (_pick_best_pool_per_chain,
                                                DS_URL, _UA, BATCH_SIZE)
            from exchanges import load_proxies
            proxies = load_proxies()
        except Exception:
            return {}
        # collect contracts (dedup, cap at 30 for one DS batch)
        contracts: list[str] = []
        seen: set[str] = set()
        for lst in group.get("listings", []):
            for _, nd in (lst.get("networks") or {}).items():
                c = (nd.get("contract") or "").strip()
                if c and len(c) > 6 and c.lower() not in seen:
                    seen.add(c.lower())
                    contracts.append(c)
                    if len(contracts) >= BATCH_SIZE:
                        break
            if len(contracts) >= BATCH_SIZE:
                break
        if not contracts:
            return {}
        url = DS_URL + ",".join(contracts)
        for _ in range(3):
            proxy = random.choice(proxies) if proxies else None
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, proxy=proxy, headers=_UA,
                                      timeout=aiohttp.ClientTimeout(total=8)) as r:
                        if r.status != 200:
                            continue
                        d = await r.json()
                pairs = d.get("pairs") or []
            except Exception:
                continue
            # group returned pairs per requested contract
            from collections import defaultdict
            per_ctr = defaultdict(list)
            wanted = {c.lower() for c in contracts}
            for p in pairs:
                addr = ((p.get("baseToken") or {}).get("address") or "").lower()
                if addr in wanted:
                    per_ctr[addr].append(p)
            out: dict[str, dict] = {}
            for c in contracts:
                best = _pick_best_pool_per_chain(per_ctr.get(c.lower()) or [], c)
                for ch, entry in best.items():
                    out[f"{ch}:{c.lower()}"] = entry
            return out
        return {}

    async def send(self, alert: dict) -> None:
        gid = alert.get("group", "?")
        text, kb = await format_alert(alert, self.currency_cache)
        if not self.app:
            log.info("ALERT (no TG): %s", text.replace("\n", " | ")[:300])
            return
        chat_ids = await self.subs.snapshot()
        if not chat_ids:
            log.info("ALERT (no subs): %s %.2f%% (%s->%s)", gid,
                     alert.get("spread", 0),
                     alert.get("buy", {}).get("eid"),
                     alert.get("sell", {}).get("eid"))
            return
        for cid in chat_ids:
            try:
                await self.app.bot.send_message(
                    chat_id=cid, text=text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=kb,
                )
            except Exception as e:
                log.warning("TG send err chat_id=%s group=%s: %s",
                            cid, gid, str(e)[:80])
