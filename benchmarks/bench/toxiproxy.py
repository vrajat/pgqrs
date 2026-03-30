"""Minimal Toxiproxy API client for benchmark setup."""

from __future__ import annotations

import json
from dataclasses import dataclass
from urllib import error, request


class ToxiproxyError(RuntimeError):
    """Raised when Toxiproxy returns an unexpected response."""


@dataclass(frozen=True)
class ToxiproxyConfig:
    api_url: str
    proxy_name: str
    listen: str
    upstream: str
    latency_ms: int
    jitter_ms: int


class ToxiproxyClient:
    def __init__(self, api_url: str) -> None:
        self.api_url = api_url.rstrip("/")

    def _request(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
        *,
        allow_not_found: bool = False,
    ) -> dict | list | None:
        data = None
        headers = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = request.Request(
            f"{self.api_url}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with request.urlopen(req) as response:
                raw = response.read().decode("utf-8")
        except error.HTTPError as exc:
            if allow_not_found and exc.code == 404:
                return None
            body = exc.read().decode("utf-8", errors="replace")
            raise ToxiproxyError(
                f"Toxiproxy API {method} {path} failed: HTTP {exc.code} {body}"
            ) from exc
        except error.URLError as exc:
            raise ToxiproxyError(
                f"Failed to reach Toxiproxy API at {self.api_url}: {exc}"
            ) from exc

        if not raw:
            return None
        return json.loads(raw)

    def ping(self) -> None:
        self._request("GET", "/proxies")

    def recreate_proxy(self, *, name: str, listen: str, upstream: str) -> None:
        self._request("DELETE", f"/proxies/{name}", allow_not_found=True)
        self._request(
            "POST",
            "/proxies",
            {
                "name": name,
                "listen": listen,
                "upstream": upstream,
            },
        )

    def add_latency(
        self,
        *,
        proxy_name: str,
        name: str,
        latency_ms: int,
        jitter_ms: int = 0,
        stream: str = "downstream",
    ) -> None:
        self._request(
            "POST",
            f"/proxies/{proxy_name}/toxics",
            {
                "name": name,
                "type": "latency",
                "stream": stream,
                "toxicity": 1.0,
                "attributes": {
                    "latency": latency_ms,
                    "jitter": jitter_ms,
                },
            },
        )
