from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import httpx

from .config import Config


@dataclass
class CacheEntry:
    path: Path
    meta_path: Path


class DiskCache:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _key(self, url: str) -> str:
        return hashlib.sha256(url.encode("utf-8")).hexdigest()

    def entry(self, url: str) -> CacheEntry:
        key = self._key(url)
        path = self.root / key
        meta_path = self.root / f"{key}.json"
        return CacheEntry(path=path, meta_path=meta_path)

    def read_meta(self, url: str) -> dict[str, str] | None:
        entry = self.entry(url)
        if not entry.meta_path.exists():
            return None
        return json.loads(entry.meta_path.read_text())  # type: ignore[no-any-return]

    def write_meta(self, url: str, meta: dict[str, str]) -> None:
        entry = self.entry(url)
        entry.meta_path.write_text(json.dumps(meta, indent=2, sort_keys=True))


class RateLimiter:
    def __init__(self, per_minute: int) -> None:
        self.interval = 60.0 / max(per_minute, 1)
        self._last_time: float | None = None

    def wait(self) -> None:
        if self._last_time is None:
            self._last_time = time.monotonic()
            return
        elapsed = time.monotonic() - self._last_time
        if elapsed < self.interval:
            time.sleep(self.interval - elapsed)
        self._last_time = time.monotonic()


def _build_client(config: Config) -> httpx.Client:
    headers = {"User-Agent": config.user_agent}
    return httpx.Client(timeout=config.timeout_seconds, headers=headers, follow_redirects=True)


def cached_get(url: str, config: Config) -> Path:
    cache = DiskCache(config.cache_dir / "http")
    entry = cache.entry(url)
    meta = cache.read_meta(url) or {}

    headers = {}
    if "etag" in meta:
        headers["If-None-Match"] = meta["etag"]
    if "last_modified" in meta:
        headers["If-Modified-Since"] = meta["last_modified"]

    with _build_client(config) as client:
        response = client.get(url, headers=headers)
        if response.status_code == 304:
            if entry.path.exists():
                return entry.path
            response = client.get(url)
        response.raise_for_status()
        entry.path.write_bytes(response.content)
        new_meta = {
            "etag": response.headers.get("etag", ""),
            "last_modified": response.headers.get("last-modified", ""),
            "url": url,
        }
        cache.write_meta(url, new_meta)
        return entry.path


def cached_head(url: str, config: Config) -> httpx.Response:
    with _build_client(config) as client:
        return client.head(url)


def download_urls(urls: Iterable[str], dest_dir: Path, config: Config) -> list[Path]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    limiter = RateLimiter(config.rate_limit_per_minute)
    downloaded: list[Path] = []
    for url in urls:
        limiter.wait()
        cache_path = cached_get(url, config)
        dest_path = dest_dir / Path(url).name
        dest_path.write_bytes(cache_path.read_bytes())
        downloaded.append(dest_path)
    return downloaded
