"""Tiny HTTP client built on the standard library.

`urllib` is enough for this harness and keeps the E2E suite dependency free.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class Response:
    """Result of an HTTP call."""

    status: int
    body: str

    def json(self) -> Any:
        return json.loads(self.body) if self.body else None


def request(
    method: str,
    url: str,
    *,
    payload: Mapping[str, Any] | None = None,
    timeout: float = 10.0,
) -> Response:
    """Performs an HTTP request; non-2xx statuses are returned instead of raised."""
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return Response(status=response.status, body=response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:  # includes 4xx/5xx bodies, which the tests assert on
        return Response(status=exc.code, body=exc.read().decode("utf-8", errors="replace"))


def get(url: str, *, timeout: float = 10.0) -> Response:
    return request("GET", url, timeout=timeout)


def post_json(url: str, payload: Mapping[str, Any], *, timeout: float = 15.0) -> Response:
    return request("POST", url, payload=payload, timeout=timeout)


def healthy(url: str, *, timeout: float = 5.0) -> bool:
    """True when the actuator health endpoint reports UP."""
    try:
        response = get(url, timeout=timeout)
    except OSError:
        return False
    if response.status != 200:
        return False
    body = response.json() or {}
    return body.get("status") == "UP"
