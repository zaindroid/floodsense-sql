"""The engine's own console, reachable from outside the private network.

The Zetaris UI listens on localhost on the host that runs the engine, and
that host has no public IP. This forwards it from a host that does, behind
one shared password, so a teammate can open the console from anywhere
without being on the tailnet.

A browser loads a whole application rather than one call, so unlike
upstream.py this is a plain reverse proxy: any path this service does not
own itself is passed through with its headers, cookies and body. All it adds
is the gate in front. The console's own login still applies behind it, and
nothing here grants a Zetaris account.

Reads and writes both go through, deliberately: the console is how
migrations and new Lightning tables get made, which is the reason to want it.
That is the opposite of the /zetaris lane, which is read-only on purpose.

The gate is optional. With UI_USER and UI_PASSWORD set it is checked in
constant time before anything is relayed; with both unset the console is
open to the internet and its own login is the only thing in front of a
writable engine.
"""

from __future__ import annotations

import hmac
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request
from base64 import b64decode
from hashlib import sha256

URL = os.environ.get("ZETARIS_UI_URL", "").strip().rstrip("/")
USER = os.environ.get("UI_USER", "").strip()
PASSWORD = os.environ.get("UI_PASSWORD", "")

COOKIE = "fs_console"
TTL_S = 12 * 3600
TIMEOUT_S = 60
REALM = 'Basic realm="FloodSense console", charset="UTF-8"'

# Hop-by-hop headers describe one connection and must not be relayed.
_DROP_REQUEST = {
    "connection",
    "keep-alive",
    "upgrade",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "content-length",
    # Asked for uncompressed, so the body can be relayed without decoding it.
    "accept-encoding",
}
_DROP_RESPONSE = {
    "connection",
    "keep-alive",
    "upgrade",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "content-length",
    "content-encoding",
    # The console labels its bundles "public, max-age=31536000, immutable".
    # Behind a CDN that is a hole rather than a saving: measured, an asset
    # fetched once with the password came back to an anonymous request with
    # cf-cache-status: HIT. Whatever is relayed here is caller-specific, so
    # the caching policy is this service's to set, not the console's.
    "cache-control",
    "expires",
    "pragma",
    "etag",
    "last-modified",
}


class Unavailable(RuntimeError):
    """No console configured, or it cannot be reached from here."""


def configured() -> bool:
    """Whether there is a console to forward at all."""
    return bool(URL)


def gated() -> bool:
    """Whether a password stands in front of it.

    Both halves unset means open to the internet, with the console's own
    login as the only lock. That is a setting rather than an oversight: it
    is reversible by setting UI_USER and UI_PASSWORD again, with no code
    change and no rebuild.
    """
    return bool(USER and PASSWORD)


def _mac(stamp: str) -> str:
    return hmac.new(PASSWORD.encode(), stamp.encode(), sha256).hexdigest()[:32]


def issue() -> str:
    """A cookie that says this browser already gave the password.

    Browsers do not reliably re-send basic credentials for every path of an
    origin, and this application fetches from the root (/_next, /assets), so
    relying on that alone logged people out at random.
    """
    stamp = str(int(time.time()))
    return f"{stamp}.{_mac(stamp)}"


def cookie_ok(value: str | None) -> bool:
    stamp, _, mac = (value or "").partition(".")
    if not stamp.isdigit() or not mac:
        return False
    if not hmac.compare_digest(mac, _mac(stamp)):
        return False
    return time.time() - int(stamp) < TTL_S


def password_ok(authorization: str | None) -> bool:
    raw = (authorization or "").strip()
    if not raw.startswith("Basic "):
        return False
    try:
        user, _, password = b64decode(raw[6:]).decode("utf-8", "replace").partition(":")
    except ValueError:
        return False
    # Both halves, so a right password with a wrong user does not get in.
    return secrets.compare_digest(user, USER) and secrets.compare_digest(password, PASSWORD)


def forward(
    method: str, path: str, query: str, headers: list[tuple[str, str]], body: bytes
) -> tuple[int, list[tuple[str, str]], bytes]:
    """Relay one request to the console and hand back what it said."""
    if not configured():
        raise Unavailable("no console configured")

    target = f"{URL}{path}" + (f"?{query}" if query else "")
    send = {k: v for k, v in headers if k.lower() not in _DROP_REQUEST}
    # The caller's own Host is kept rather than replaced with the upstream
    # address. The console is a Next application, and its form submits are
    # server actions, which refuse a request whose Origin does not match the
    # host it believes it is serving -- replacing Host breaks the login while
    # leaving every page and asset working, so it fails in the one place
    # nobody tests first.
    # Only the gate's own credential stops here. Everything else is the
    # console's: after signing in, its pages carry the engine's token as
    # "Authorization: Bearer ...", and dropping that header wholesale meant
    # the engine saw every call unauthenticated and answered 401, which the
    # console showed as a failed login -- a login that had in fact succeeded.
    auth = send.get("authorization", "")
    if auth.strip().lower().startswith("basic "):
        send.pop("authorization", None)

    host = send.get("host") or urllib.parse.urlsplit(URL).netloc
    send["Host"] = host
    send.setdefault("X-Forwarded-Host", host)
    send.setdefault("X-Forwarded-Proto", "https")
    req = urllib.request.Request(target, data=body or None, headers=send, method=method)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_S) as r:  # noqa: S310 - fixed host
            status, raw, payload = r.status, r.headers.items(), r.read()
    except urllib.error.HTTPError as e:
        # A 4xx from the console is its answer, not a failure to relay.
        status, raw, payload = e.code, e.headers.items(), e.read()
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        raise Unavailable(str(e)[:200]) from None

    out: list[tuple[str, str]] = []
    for k, v in raw:
        if k.lower() in _DROP_RESPONSE:
            continue
        # Should the console ever answer with its own absolute address, it is
        # one nobody outside can reach.
        if k.lower() == "location" and v.startswith(URL):
            v = v[len(URL) :] or "/"
        out.append((k, v))
    return status, out, payload
