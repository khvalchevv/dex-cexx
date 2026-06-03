"""Persistent token blacklist. Tokens added here are skipped by the scanner."""
import asyncio
import json
import logging
import os

log = logging.getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
BLACKLIST_FILE = os.path.join(_HERE, "blacklist.json")


class TokenBlacklist:
    def __init__(self, path: str = BLACKLIST_FILE):
        self.path = path
        self.tokens: set[str] = set()
        self.lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            self.tokens = set(str(x).upper() for x in data.get("tokens", []))
            log.info("blacklist loaded: %d tokens", len(self.tokens))
        except FileNotFoundError:
            self.tokens = set()

    def _save(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"tokens": sorted(self.tokens)}, f)
        except Exception as e:
            log.warning("blacklist save err: %s", e)

    async def add(self, token: str) -> bool:
        async with self.lock:
            t = token.upper()
            if t in self.tokens:
                return False
            self.tokens.add(t)
            self._save()
            return True

    async def remove(self, token: str) -> bool:
        async with self.lock:
            t = token.upper()
            if t not in self.tokens:
                return False
            self.tokens.discard(t)
            self._save()
            return True

    def contains_sync(self, token: str) -> bool:
        """Read-only snapshot — safe to call from scanner hot loop."""
        return token.upper() in self.tokens

    async def snapshot(self) -> set[str]:
        async with self.lock:
            return set(self.tokens)
