"""Telegram alerter — broadcast model, clean formatting, action buttons,
ticker-collision filter via contract-address -> CoinGecko cg_id matching.
"""
import asyncio
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
    """Deposit/withdraw status pair: 🟢 open, 🔴 closed, ❓ unknown."""
    e = lambda b: "🟢" if b is True else ("🔴" if b is False else "❓")
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


def _cex_block(eid: str, price_str: str, nets: dict, net_limit: int = 6) -> list[str]:
    """Tree-style CEX rendering:
        EXCHANGE  $price | 🟢🟢            (aggregate deposit/withdraw)
          └ bep20: 🟢🟢                    (per-network deposit/withdraw)
    Withdraw-open networks first so the usable ones survive truncation."""
    dep, wd = _agg_dw(nets)
    price_part = f" {price_str}" if price_str else ""
    lines = [f"<b>{eid.upper()}</b>{price_part} | {_dw(dep, wd)}"]
    items = sorted(nets.items(),
                   key=lambda kv: (kv[1].get("withdraw") is not True,
                                   kv[1].get("deposit") is not True, kv[0]))
    for net, nd in items[:net_limit]:
        lines.append(f"  └ {net.lower()}: {_dw(nd.get('deposit'), nd.get('withdraw'))}")
    if len(items) > net_limit:
        lines.append(f"  └ +{len(items) - net_limit} more")
    return lines


def _venue_label(v: dict) -> str:
    if v["kind"] == "dex":
        return f"{(v.get('dex') or 'dex')}_{v.get('chain') or ''}"
    return v["eid"].upper()


_OKX_URL_SLUG = {
    "ethereum": "ethereum", "bsc": "bsc", "solana": "solana", "base": "base",
    "arbitrum": "arbitrum", "polygon": "polygon", "optimism": "optimism",
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
        liq = _fmt_liq(v.get("liq", 0))
        out = [f"<b>{label}:</b> {(v.get('chain') or 'dex').upper()} (DEX)  "
               f"{price}  (vol {liq})"]
        if v.get("contract"):
            out.append(f"<code>{v['contract']}</code>")
        return out
    blk = _cex_block(v["eid"], price, nets_by_eid.get(v["eid"]) or {})
    blk[0] = f"<b>{label}:</b> " + blk[0]
    return blk


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
    P.append(f"‼️ <b>{display}</b> / +{spread:.2f}%")
    P.append(f"<b>Route:</b> {vlabel(buy)} → {vlabel(sell)}")
    P.append(f"🕐 {ts}")
    P.append("<i>D/W: 🟢 open · 🔴 closed · ❓ n/a (deposit, withdraw)</i>")
    P.append("")
    P.extend(_leg_lines("Buy", buy, "buy", nets_by_eid))
    P.extend(_leg_lines("Sell", sell, "sell", nets_by_eid))

    # other CEX — every CEX listing except the legs, with price + D/W
    legs = {v["eid"] for v in (buy, sell) if v["kind"] == "cex"}
    others = [q for q in cex if q["eid"] not in legs]
    if others:
        P.append("")
        P.append("<b>other CEX:</b>")
        for q in sorted(others, key=lambda x: x["ask"])[:12]:
            nets = nets_by_eid.get(q["eid"]) or {}
            P.extend(_cex_block(q["eid"], _fmt_price(q["ask"]), nets, net_limit=5))

    # other DEX chains (besides the leg) — price + vol
    leg_chains = {v.get("chain") for v in (buy, sell) if v["kind"] == "dex"}
    odex = [e for e in dex if e.get("chain") not in leg_chains]
    if odex:
        P.append("")
        P.append("<b>other DEX:</b>")
        for e in sorted(odex, key=lambda x: -(x.get("vol24h") or 0))[:8]:
            P.append(f"  {(e.get('chain') or '').upper()}: {_fmt_price(e['priceUsd'])}"
                     f"  (vol {_fmt_liq(e.get('vol24h', 0))})")

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
    P.append(f"<b>Project:</b> {display}")
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
    rows.append([InlineKeyboardButton(f"🚫 Blacklist {display[:24]}",
                                      callback_data=f"bl:{gid.partition('#')[0]}")])
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
                  resolver=None):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        self.currency_cache = currency_cache
        self.subs = SubscriberStore()
        self.blacklist = blacklist or TokenBlacklist()
        self.resolver = resolver
        self.app: Application | None = None
        if not self.token:
            log.warning("TELEGRAM_BOT_TOKEN missing — alerts will be logged only")
            return
        self.app = Application.builder().token(self.token).build()
        self.app.add_handler(CommandHandler("start", self._cmd_start))
        self.app.add_handler(CommandHandler("stop", self._cmd_stop))
        self.app.add_handler(CommandHandler("status", self._cmd_status))
        self.app.add_handler(CommandHandler("blacklist", self._cmd_blacklist))
        self.app.add_handler(CommandHandler("unblacklist", self._cmd_unblacklist))
        self.app.add_handler(CommandHandler("check", self._cmd_check))
        self.app.add_handler(CallbackQueryHandler(self._cb_blacklist, pattern=r"^bl:"))

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
