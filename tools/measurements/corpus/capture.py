"""Record and replay the fetch lane's in-lane HTTP surfaces (S4/S5).

Three surfaces, because the fetch lane reaches the network through three of them and
capturing only the first leaves replay unusable:

``fetch_text``
    the seam ``resolve_fetch_text_impl`` already short-circuits (``transport.py:542``):
    supplying a custom callable makes every adapter fetch go through it.
    ``run_pipeline(fetch_text=...)`` is the documented way in, so no adapter and no
    product seam changes.

``urlopen``
    what ``common/http.default_fetch_text`` and ``common/url`` use, plus four adapter
    leaves that imported it directly (``html_parsers``, ``social``,
    ``provider_structured_listing``, ``jobylon_v1``). Intercepting here rather than only at
    ``fetch_text`` is what preserves status, response headers and the post-redirect URL,
    which is exactly what ``resolve_supported_redirect_url`` reads.

``httpx.Client``
    what the ETag/Last-Modified revalidation and the pooled redirect resolver use. Without
    it, a replayed static source raises instead of replaying.

Fidelity rules that decide whether a replay matches at all
---------------------------------------------------------

* Bodies are stored as the **decoded text**, re-encoded as UTF-8, and a replayed response
  advertises ``charset=utf-8``. A recorded ``windows-1252`` page therefore round-trips
  byte-for-byte as text instead of being decoded twice into mojibake.
* A recorded 4xx/5xx is replayed as an ``httpx`` **response**, not an exception, because
  that is what the live async-free httpx path does: ``PooledRedirectResolver`` reads
  ``response.url`` from a 404 and the revalidation helper reads ``response.headers``.
  Only recorded *failures* (transport errors) raise on replay.
* Supplying a custom ``fetch_text`` short-circuits ``resolve_fetch_text_impl``, so the
  **synchronous** per-source lane is uniform between arms, and ``AsyncHttpTextFetcher`` --
  ``adapter_http_concurrency``'s only in-process consumer -- is never constructed in either
  arm. That is not a detail: it is why a replay A/B cannot see a claim about HTTP pool sizing
  (S0 says so in its verdict), and it is the reason the async lane is listed as out of scope
  below rather than quietly measured.
* The static listing lane *does* replay: its page fetches go through the sync seam, so they are
  captured. What broke it in the first real replay was this module's own guard, which blocked
  the loopback ``socketpair`` asyncio builds for its self-pipe -- every ``asyncio.run`` in the
  lane died and four static sources failed against the guard instead of the corpus. Allowing
  that one call fixed replay fidelity for them and took the guard to a literal ``blocked == 0``.

The egress backstop
-------------------

``install_egress_guard()`` blocks ``socket.create_connection``, ``socket.connect`` and
``socket.connect_ex``, so "zero egress" is proven by construction: a replay that finishes with
``blocked == 0`` demonstrably never opened a socket, including through a path this module does
not know about. Anything that tries is named and raises :class:`EgressBlocked` rather than
quietly fetching live data into a hermetic measurement. The single exception is
``socket.socketpair()`` -- a kernel-local pair with no peer, which is how asyncio's event loop
is built on Windows. It is allowed and counted separately (``socketpairAllowed``); a connect to
a listening loopback *server* is still blocked, which is the case that matters when a recorded
board is local.

Dev-only by construction: this module lives in ``tools/`` and patches *its own process*,
which is what a replay harness is. It adds nothing to the product and changes no product
seam.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Callable, Iterator, Mapping, Sequence
from http.client import InvalidURL
from typing import Any, NoReturn
from urllib.error import HTTPError, URLError

from src.jobs.common.http import HttpStatusError, fetch_max_bytes_for_url
from src.jobs.transport import default_fetch_text as transport_default_fetch_text
from tools.measurements.corpus.store import (
    CorpusMiss,
    CorpusStore,
    RequestLedger,
)

SURFACE_FETCH_TEXT = "fetch_text"
SURFACE_URLOPEN = "urlopen"
SURFACE_HTTPX = "httpx"
ALL_SURFACES: tuple[str, ...] = (SURFACE_FETCH_TEXT, SURFACE_URLOPEN, SURFACE_HTTPX)

# In-lane modules that did ``from urllib.request import urlopen`` and therefore hold their
# own reference: patching only ``urllib.request`` would miss every one of them.
_URLOPEN_MODULE_PATHS: tuple[str, ...] = (
    "src.jobs.common.http",
    "src.jobs.common.url",
    "src.jobs.adapters.html_parsers",
    "src.jobs.adapters.social",
    "src.jobs.adapters.provider_structured_listing",
    "src.scrapers.providers.jobylon_v1",
)
# Named so the coverage report states what is *not* intercepted by name rather than by
# omission. Each is either outside the fetch lane or caught by the guard instead.
OUT_OF_LANE_NETWORK_CALLERS: tuple[str, ...] = (
    "src/jobs/browser_fallback_pool.py (Node/Playwright subprocess, D9)",
    "src/source_discovery/** (separate subsystem and transport, D6/D9)",
    "src/bridge/** (admin registry API, outside the fetch lane)",
    "src/jobs/availability_validator.py (run in-process only when availability validation runs)",
    "src/jobs/transport.py AsyncHttpTextFetcher (never constructed when fetch_text is custom, "
    "so its pool sizing is unreachable in replay -- the S0 verdict relies on this)",
)


class EgressBlocked(RuntimeError):
    """An outbound socket was attempted while the replay guard was armed."""

    def __init__(self, target: str) -> None:
        self.target = str(target)
        super().__init__(f"egress blocked during replay: {self.target}")


class EgressGuard:
    """Refuses every outbound socket and counts the attempts: zero egress by construction.

    One thing is deliberately *not* egress: ``socket.socketpair()``, which asyncio's event loop
    builds as its self-pipe. On Windows that is implemented by binding and connecting
    ``127.0.0.1``, so a guard that blocked it killed every ``asyncio.run`` -- a real subset
    replay proved it by failing four static sources against the guard, and a lane that cannot
    start an event loop cannot be replayed from the corpus either. The socketpair call is
    therefore allowed (and counted as ``socketpairAllowed``) while every other outbound
    connect still raises, loopback included: a replay whose board is a loopback server must
    fail on a live fetch, and a connect to a listening loopback port is that.

    The counter is classified because it is read as evidence: ``blockedRemote`` is the egress
    figure a hermetic replay has to keep at zero, and ``loopbackBlocked`` names the in-process
    attempts without pretending they are the same thing.

    The socketpair allowance is **per thread**, because the guard is armed around a replay whose
    worker pool builds an event loop per source. A process-global flag was measured wrong: in a
    threaded socketpair loop 11 of 12 threads were blocked, and a full-source birth replay failed
    its static sources (``egress blocked during replay: 127.0.0.1:<port>``) that its record had
    fetched fine. One thread's connect fell outside another thread's socketpair window. A depth,
    not a flag, so a socketpair built inside another closes back to the outer allowance.
    """

    _LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost", "ip6-localhost"}

    def __init__(self) -> None:
        self.blocked = 0
        self.loopback_blocked = 0
        self.remote_blocked = 0
        self.socketpair_allowed = 0
        self.targets: list[str] = []
        self._socketpair_depth = threading.local()
        self._saved: list[tuple[Any, str, Any]] = []

    def _socketpair_allowance_open(self) -> bool:
        return bool(getattr(self._socketpair_depth, "depth", 0))

    def _enter_socketpair(self) -> None:
        self._socketpair_depth.depth = getattr(self._socketpair_depth, "depth", 0) + 1

    def _leave_socketpair(self) -> None:
        depth = getattr(self._socketpair_depth, "depth", 0) - 1
        if depth > 0:
            self._socketpair_depth.depth = depth
            return
        try:
            del self._socketpair_depth.depth
        except AttributeError:  # pragma: no cover - nothing left to clear
            return

    @staticmethod
    def _describe(address: Any) -> str:
        if isinstance(address, tuple) and len(address) >= 2:
            return f"{address[0]}:{address[1]}"
        return str(address)

    @classmethod
    def _is_loopback(cls, address: Any) -> bool:
        host = address[0] if isinstance(address, tuple) and address else address
        return str(host).strip().lower() in cls._LOOPBACK_HOSTS

    def _block(self, address: Any) -> NoReturn:
        target = self._describe(address)
        self.blocked += 1
        if self._is_loopback(address):
            self.loopback_blocked += 1
        else:
            self.remote_blocked += 1
        if len(self.targets) < 50:
            self.targets.append(target)
        raise EgressBlocked(target)

    def __enter__(self) -> EgressGuard:
        real_socket = socket.socket
        real_socketpair = socket.socketpair
        real_create_connection = socket.create_connection
        guard = self

        class _GuardedSocket(real_socket):  # type: ignore[misc, valid-type]
            def connect(self, address: Any, *args: Any, **kwargs: Any) -> Any:
                if guard._socketpair_allowance_open():
                    guard.socketpair_allowed += 1
                    return real_socket.connect(self, address, *args, **kwargs)
                guard._block(address)

            def connect_ex(self, address: Any, *args: Any, **kwargs: Any) -> int:
                if guard._socketpair_allowance_open():
                    guard.socketpair_allowed += 1
                    return real_socket.connect_ex(self, address, *args, **kwargs)
                guard._block(address)

        def _guarded_create_connection(address: Any, *args: Any, **kwargs: Any) -> Any:
            guard._block(address)

        def _guarded_socketpair(*args: Any, **kwargs: Any) -> Any:
            guard._enter_socketpair()
            try:
                return real_socketpair(*args, **kwargs)
            finally:
                guard._leave_socketpair()

        self._saved = [
            (socket, "socket", real_socket),
            (socket, "create_connection", real_create_connection),
            (socket, "socketpair", real_socketpair),
        ]
        socket.socket = _GuardedSocket  # type: ignore[assignment]
        socket.create_connection = _guarded_create_connection  # type: ignore[assignment]
        socket.socketpair = _guarded_socketpair  # type: ignore[assignment]
        return self

    def __exit__(self, *exc_info: Any) -> None:
        for module, name, original in self._saved:
            setattr(module, name, original)
        self._saved = []

    def as_report(self) -> dict[str, Any]:
        return {
            "guarded": True,
            "blocked": self.blocked,
            "blockedRemote": self.remote_blocked,
            "blockedLoopback": self.loopback_blocked,
            "socketpairAllowed": self.socketpair_allowed,
            "targets": list(self.targets),
        }


def install_egress_guard() -> EgressGuard:
    """Backstop, used as ``with install_egress_guard() as guard:``."""

    return EgressGuard()


# ── header and body helpers ─────────────────────────────────────────────────


def _headers_pairs(headers: Any) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    try:
        items = list(headers.items()) if headers is not None else []
    except Exception:
        return pairs
    for name, value in items:
        pairs.append((str(name), str(value)))
    return pairs


def _response_charset(headers: Any) -> str:
    getter = getattr(headers, "get_content_charset", None)
    try:
        charset = getter() if callable(getter) else None
    except Exception:
        charset = None
    return str(charset or "") or "utf-8"


def _request_headers(request: Any) -> dict[str, str]:
    if request is None:
        return {}
    raw = getattr(request, "headers", None)
    if isinstance(raw, Mapping):
        return {str(name): str(value) for name, value in raw.items()}
    if isinstance(request, Mapping):
        headers = request.get("headers")
        if isinstance(headers, Mapping):
            return {str(name): str(value) for name, value in headers.items()}
    return {}


def _status_code(exc: BaseException) -> int:
    code = getattr(exc, "code", None)
    if isinstance(code, int) and not isinstance(code, bool) and code > 0:
        return int(code)
    message = str(exc)
    for prefix in ("HTTP ", "HTTP Error "):
        if prefix in message:
            tail = message.split(prefix, 1)[1].strip().split(" ", 1)[0]
            if tail.isdigit():
                return int(tail)
    return 0


def _entry_header_map(entry: Mapping[str, Any]) -> dict[str, str]:
    """Last value per response header, with the body charset pinned to UTF-8.

    Bodies are stored as decoded text re-encoded as UTF-8, so a replayed response must
    advertise ``charset=utf-8`` even when the board served ``windows-1252``; otherwise the
    consumer decodes our UTF-8 bytes with the board's charset and produces mojibake.
    """

    headers: dict[str, str] = {}
    for name, values in (entry.get("responseHeaders") or {}).items():
        sequence = values if isinstance(values, list) else [values]
        if not sequence:
            continue
        headers[str(name)] = str(sequence[-1])
    content_type = headers.get("content-type") or headers.get("Content-Type")
    if content_type:
        media_type = str(content_type).split(";", 1)[0].strip() or "text/plain"
        headers["content-type"] = f"{media_type}; charset=utf-8"
    else:
        headers["content-type"] = "text/plain; charset=utf-8"
    return headers


def _entry_header_pairs(entry: Mapping[str, Any]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for name, values in (entry.get("responseHeaders") or {}).items():
        sequence = values if isinstance(values, list) else [values]
        for value in sequence:
            pairs.append((str(name), str(value)))
    return pairs


class _ReplayHeaders(dict):  # type: ignore[type-arg]
    """Case-insensitive multi-value response headers, shaped like ``http.client``'s."""

    def __init__(self, pairs: Sequence[tuple[str, str]] = ()) -> None:
        super().__init__()
        for name, value in pairs:
            self.add(name, value)

    def add(self, name: str, value: str) -> None:
        bucket = dict.setdefault(self, str(name).lower(), [])
        bucket.append(str(value))

    def get(self, name: str, default: Any = None) -> Any:  # type: ignore[override]
        values = dict.get(self, str(name).lower())
        return values[0] if values else default

    def get_all(self, name: str) -> list[str]:
        return list(dict.get(self, str(name).lower()) or [])

    def items(self) -> Iterator[tuple[str, str]]:  # type: ignore[override]
        for name, values in list(dict.items(self)):
            for value in values:
                yield (name, value)

    def get_content_charset(self) -> str | None:
        """Always ``utf-8``: bodies are served as the stored text, encoded as UTF-8."""

        return "utf-8"


class _ReplayUrlopenResponse:
    """What ``urlopen`` hands back, rebuilt from a corpus entry. No sockets involved."""

    def __init__(
        self,
        *,
        url: str,
        resolved_url: str,
        status: int,
        headers: Sequence[tuple[str, str]],
        body: bytes,
    ) -> None:
        self.url = str(url)
        self._resolved_url = str(resolved_url or url)
        self.status = int(status or 200)
        self.code = self.status
        self.headers = _ReplayHeaders(headers)
        self._body = bytes(body)
        self._offset = 0

    def read(self, amount: int = -1) -> bytes:
        if amount is None or int(amount) < 0:
            chunk = self._body[self._offset :]
            self._offset = len(self._body)
            return chunk
        chunk = self._body[self._offset : self._offset + int(amount)]
        self._offset += len(chunk)
        return chunk

    def geturl(self) -> str:
        return self._resolved_url

    def getcode(self) -> int:
        return self.status

    def info(self) -> Any:
        return self.headers

    def close(self) -> None:
        return None

    def __enter__(self) -> _ReplayUrlopenResponse:
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()


def raise_for_entry(entry: Mapping[str, Any], *, httpx_module: Any = None) -> NoReturn:
    """Replay a recorded failure as the exception it was.

    Never returns, and never substitutes an empty body: a replayed failure arriving as
    ``""`` would be indistinguishable from a board with no openings, which is the exact
    confusion this layer exists to prevent.
    """

    error = entry.get("error") or {}
    name = str(error.get("class") or "RuntimeError")
    message = str(error.get("message") or "")
    code = int(error.get("code") or 0)
    url = str(entry.get("url") or "")

    if httpx_module is not None:
        candidate = getattr(httpx_module, name, None)
        if isinstance(candidate, type) and issubclass(candidate, BaseException):
            raise candidate(message or f"recorded {name} for {url}")
    # The recorded class wins over the status code. Ordering these the other way round (a
    # truthy ``code`` meaning ``HttpStatusError``) made a replayed ``urlopen`` 404 arrive as
    # ``HttpStatusError`` -- a different type from the ``HTTPError`` the live run raised, so a
    # caller's own ``except HTTPError`` stopped matching and the loader failed. A code is only
    # a fallback for entries whose class this module does not know how to rebuild.
    factories: dict[str, Callable[[], BaseException]] = {
        "HttpStatusError": lambda: HttpStatusError(code or 0, url),
        "HTTPError": lambda: HTTPError(url, code, message or "recorded HTTP error", {}, None),
        "URLError": lambda: URLError(message or f"recorded network error for {url}"),
        "TimeoutError": lambda: TimeoutError(message or f"recorded timeout for {url}"),
        "socket.timeout": lambda: TimeoutError(message or f"recorded timeout for {url}"),
        # Observed live: a static board's templated detail link reached urllib and raised this.
        # Rebuilt as itself rather than falling through to RuntimeError, or a caller's own
        # ``except InvalidURL`` would stop matching -- the same defect this table exists to fix.
        "InvalidURL": lambda: InvalidURL(message or f"recorded invalid URL for {url}"),
    }
    factory = factories.get(name)
    if factory is not None:
        raise factory()
    if code:
        raise HttpStatusError(code, url)
    raise RuntimeError(message or f"recorded failure for {url}")


# ── surface patches ─────────────────────────────────────────────────────────


class _UrlopenPatch:
    """Replaces ``urlopen`` wherever the fetch lane imported it, remembering the original.

    The original is captured *before* anything is replaced, so a handler that needs the
    live callable cannot accidentally call itself.
    """

    def __init__(self, handler: Callable[..., Any]) -> None:
        self.handler = handler
        self.real: Callable[..., Any] | None = None
        self._targets: list[tuple[Any, str, Any]] = []

    def __enter__(self) -> _UrlopenPatch:
        import importlib
        import urllib.request as urllib_request

        targets: list[tuple[Any, str, Any]] = [(urllib_request, "urlopen", urllib_request.urlopen)]
        self.real = urllib_request.urlopen
        for module_path in _URLOPEN_MODULE_PATHS:
            try:
                module = importlib.import_module(module_path)
            except ImportError:
                continue
            original = getattr(module, "urlopen", None)
            if original is not None:
                targets.append((module, "urlopen", original))
        for module, name, _original in targets:
            setattr(module, name, self.handler)
        self._targets = targets
        return self

    def __exit__(self, *exc_info: Any) -> None:
        for module, name, original in self._targets:
            setattr(module, name, original)
        self._targets = []


class _BuildOpenerPatch:
    """Intercepts an opener an adapter builds for itself by patching ``build_opener``.

    ``urllib.request.build_opener(...).open(...)`` is not the module-level ``urlopen``: an adapter
    that keeps its own opener -- with its own cookie jar and handlers -- reaches the network
    without ever touching the patched ``urlopen``. The dayforce plugin is the measured case: it
    builds a private opener over stdlib ``urllib`` and calls ``opener.open``, so no offer for it
    appeared in ``offersBySurface`` and ``unattributedCount == 0`` read as "the record captured all
    traffic" when it had not. A birth replay of an epoch recorded that way fails on it plainly:
    ``egress blocked during replay: jobs.dayforcehcm.com:443``.

    The class method (``OpenerDirector.open``) is deliberately *not* what is replaced. ``urlopen``
    reaches the network through the module-level opener's own ``.open``, so patching the class
    would re-enter this handler from inside the live call. Shadowing ``open`` on each opener this
    factory returns is per object, leaves the module opener ``urlopen`` uses alone, and keeps the
    live call running through the opener the caller actually configured.
    """

    def __init__(self, handler: Callable[..., Any]) -> None:
        self.handler = handler
        self.real: Callable[..., Any] | None = None

    def __enter__(self) -> _BuildOpenerPatch:
        import urllib.request as urllib_request

        self.real = urllib_request.build_opener
        urllib_request.build_opener = self._build
        return self

    def __exit__(self, *exc_info: Any) -> None:
        import urllib.request as urllib_request

        if self.real is not None:
            urllib_request.build_opener = self.real
        self.real = None

    def _build(self, *handlers: Any) -> Any:
        if self.real is None:  # pragma: no cover - the patch is only live inside the context
            raise RuntimeError("build_opener used outside install()")
        opener = self.real(*handlers)
        # The opener's own bound ``open``, captured before it is shadowed: the live call has to go
        # through *this* opener, because its cookie jar and handlers are why the board answers.
        real_open = opener.open

        # ``urlopen`` calls ``opener.open(url, data, timeout)`` positionally and a caller may pass
        # ``timeout=`` as a keyword, so nothing here may assume which: the arguments are forwarded
        # as they arrived.
        def intercepted(fullurl: Any, *args: Any, **kwargs: Any) -> Any:
            return self.handler(real_open, fullurl, *args, **kwargs)

        opener.open = intercepted
        return opener


class _HttpClientPatch:
    """Replaces ``httpx.Client`` with a factory built around the real class."""

    def __init__(self, factory_builder: Callable[[Any], Any]) -> None:
        self._factory_builder = factory_builder
        self.real_client: Any = None

    def __enter__(self) -> _HttpClientPatch:
        import httpx

        self.real_client = httpx.Client
        httpx.Client = self._factory_builder(self.real_client)  # type: ignore[assignment]
        return self

    def __exit__(self, *exc_info: Any) -> None:
        if self.real_client is not None:
            import httpx

            httpx.Client = self.real_client  # type: ignore[assignment]


class _CaptureBase:
    """Shared surface bookkeeping for the recorder and the replayer."""

    def __init__(
        self,
        store: CorpusStore,
        *,
        scope: str = "",
        ledger: RequestLedger | None = None,
        surfaces: Sequence[str] = ALL_SURFACES,
    ) -> None:
        self.store: CorpusStore = store
        self.scope = str(scope)
        self.ledger = ledger if ledger is not None else RequestLedger()
        self.surfaces = tuple(surfaces)
        self._patchers: list[Any] = []

    # The three seams, implemented by each side.
    def fetch_text(self, url: str, timeout_s: int, *args: Any, **kwargs: Any) -> str:
        raise NotImplementedError

    def urlopen(self, url: Any, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def opener_open(self, real_open: Any, url: Any, *args: Any, **kwargs: Any) -> Any:
        """A privately built opener's call. Same lane as ``urlopen`` unless a side overrides it."""

        return self.urlopen(url, *args, **kwargs)

    def httpx_client_factory(self, real_client: Any) -> Any:
        raise NotImplementedError

    def on_urlopen_patch(self, real_urlopen: Callable[..., Any] | None) -> None:
        return None

    def __enter__(self) -> _CaptureBase:
        if SURFACE_URLOPEN in self.surfaces:
            patcher = _UrlopenPatch(self.urlopen)
            patcher.__enter__()
            self._patchers.append(patcher)
            self.on_urlopen_patch(patcher.real)
            # Same lane, second door: an opener an adapter builds for itself is not the module-level
            # ``urlopen``, so it needs its own patch or its offers never reach the ledger.
            opener_patcher = _BuildOpenerPatch(self.opener_open)
            opener_patcher.__enter__()
            self._patchers.append(opener_patcher)
        if SURFACE_HTTPX in self.surfaces:
            httpx_patcher = _HttpClientPatch(self.httpx_client_factory)
            httpx_patcher.__enter__()
            self._patchers.append(httpx_patcher)
        return self

    def __exit__(self, *exc_info: Any) -> None:
        while self._patchers:
            self._patchers.pop().__exit__(*exc_info)

    def coverage_extra(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "surfaces": list(self.surfaces),
            "urlopenTargets": list(_URLOPEN_MODULE_PATHS),
            "privateOpeners": (
                "urllib.request.build_opener results are intercepted for the life of the capture "
                "(each opener's own ``open`` is shadowed, so an adapter's cookie jar and handlers "
                "still run); an opener built before the capture began, or via a module that imported "
                "``build_opener`` by name, is not"
            ),
            "outOfLaneNetworkCallers": list(OUT_OF_LANE_NETWORK_CALLERS),
            "asyncTransportLane": (
                "not exercised: a custom fetch_text short-circuits resolve_fetch_text_impl, "
                "so record and replay run the same workload"
            ),
        }


class CaptureRecorder(_CaptureBase):
    """Records every intercepted request, then behaves exactly as the live call did."""

    def __init__(
        self,
        store: CorpusStore,
        *,
        scope: str = "",
        ledger: RequestLedger | None = None,
        surfaces: Sequence[str] = ALL_SURFACES,
        live_fetch_text: Callable[..., str] = transport_default_fetch_text,
    ) -> None:
        super().__init__(store, scope=scope, ledger=ledger, surfaces=surfaces)
        self._live_fetch_text = live_fetch_text
        self._real_urlopen: Callable[..., Any] | None = None

    def on_urlopen_patch(self, real_urlopen: Callable[..., Any] | None) -> None:
        self._real_urlopen = real_urlopen

    # ── fetch_text seam ─────────────────────────────────────────────────────

    def fetch_text(self, url: str, timeout_s: int, *args: Any, **kwargs: Any) -> str:
        request = kwargs.get("request", args[0] if args else None)
        headers = _request_headers(request)
        self.ledger.note_offer(url, surface=SURFACE_FETCH_TEXT)
        try:
            text = self._live_fetch_text(url, timeout_s, request=request)
        except Exception as exc:
            self.store.record(
                url=url,
                request_headers=headers,
                outcome="failure",
                error=exc,
                status_code=_status_code(exc),
                surface=SURFACE_FETCH_TEXT,
                ledger=self.ledger,
            )
            raise
        self.store.record(
            url=url,
            request_headers=headers,
            outcome="ok",
            text=text,
            surface=SURFACE_FETCH_TEXT,
            ledger=self.ledger,
        )
        return text

    # ── urllib ──────────────────────────────────────────────────────────────

    def urlopen(self, url: Any, *args: Any, **kwargs: Any) -> Any:
        if self._real_urlopen is None:  # pragma: no cover - patch always supplies it
            raise RuntimeError("recorder used outside install(): no live urlopen")
        return self._urlopen_with(self._real_urlopen, url, *args, **kwargs)

    def opener_open(
        self, real_open: Callable[..., Any], url: Any, *args: Any, **kwargs: Any
    ) -> Any:
        """A privately built opener's call: recorded, and made live *through that opener*.

        The live call must not be rerouted through the module-level ``urlopen``. An adapter that
        keeps its own opener does so for its handlers and cookie jar, which are part of why the
        board answers; changing the call would change what the run fetches, and a record that is
        not the run it says it is cannot referee anything.
        """

        return self._urlopen_with(real_open, url, *args, **kwargs)

    def _urlopen_with(
        self, live_call: Callable[..., Any], url: Any, *args: Any, **kwargs: Any
    ) -> Any:
        request_url = str(getattr(url, "full_url", None) or url)
        method = str(getattr(url, "method", None) or "GET")
        headers = _request_headers(url)
        self.ledger.note_offer(request_url, surface=SURFACE_URLOPEN)
        try:
            response = live_call(url, *args, **kwargs)
        except HTTPError as exc:
            self.store.record(
                url=request_url,
                method=method,
                request_headers=headers,
                outcome="failure",
                status_code=int(exc.code or 0),
                response_headers=_headers_pairs(getattr(exc, "headers", None)),
                error=exc,
                surface=SURFACE_URLOPEN,
                ledger=self.ledger,
            )
            raise
        except Exception as exc:
            self.store.record(
                url=request_url,
                method=method,
                request_headers=headers,
                outcome="failure",
                error=exc,
                surface=SURFACE_URLOPEN,
                ledger=self.ledger,
            )
            raise
        with response:
            charset = _response_charset(getattr(response, "headers", None))
            cap = fetch_max_bytes_for_url(request_url)
            body = bytes(response.read(cap + 1)[:cap])
            pairs = _headers_pairs(getattr(response, "headers", None))
            resolved = str(getattr(response, "geturl", lambda: request_url)() or request_url)
            status = int(getattr(response, "status", 200) or 200)
        text = body.decode(charset, errors="replace")
        self.store.record(
            url=request_url,
            method=method,
            request_headers=headers,
            outcome="ok",
            text=text,
            charset=charset,
            status_code=status,
            response_headers=pairs,
            redirect_chain=[resolved] if resolved != request_url else [],
            surface=SURFACE_URLOPEN,
            ledger=self.ledger,
        )
        # The live read is consumed above, so the caller gets the same bytes back through
        # the replay-shaped response. Text is stored, so UTF-8 is what comes out.
        return _ReplayUrlopenResponse(
            url=request_url,
            resolved_url=resolved,
            status=status,
            headers=pairs,
            body=text.encode("utf-8"),
        )

    # ── httpx ───────────────────────────────────────────────────────────────

    def httpx_client_factory(self, real_client: Any) -> Any:
        recorder = self

        class _RecordingClient(real_client):  # type: ignore[misc, valid-type]
            def request(self, method: str, url: Any, **kwargs: Any) -> Any:
                call_headers = dict(kwargs.pop("headers", None) or {})
                client_headers = {
                    str(name): str(value) for name, value in dict(self.headers).items()
                }
                merged = {**client_headers, **call_headers}
                recorder.ledger.note_offer(str(url), surface=SURFACE_HTTPX)
                try:
                    response = super().request(method, url, headers=call_headers or None, **kwargs)
                except Exception as exc:
                    recorder.store.record(
                        url=str(url),
                        method=str(method),
                        request_headers=merged,
                        outcome="failure",
                        status_code=_status_code(exc),
                        error=exc,
                        surface=SURFACE_HTTPX,
                        ledger=recorder.ledger,
                    )
                    raise
                # The entry is keyed by the URL that was *asked for*, with the post-redirect
                # URL in the chain. Keying it by ``response.url`` instead made every
                # redirecting request unattributable in the coverage report and unreplayable:
                # replay looks up the URL it was asked for and missed. The redirect pool (the
                # httpx caller that matters here) always asks for the pre-redirect URL.
                resolved = str(getattr(response, "url", url) or url)
                chain = [str(str(item.url)) for item in getattr(response, "history", ()) or ()]
                if resolved and resolved != str(url):
                    chain.append(resolved)
                recorder.store.record(
                    url=str(url),
                    method=str(method),
                    request_headers=merged,
                    outcome="ok",
                    text=response.text,
                    status_code=int(getattr(response, "status_code", 0) or 0),
                    response_headers=_headers_pairs(getattr(response, "headers", None)),
                    redirect_chain=chain,
                    surface=SURFACE_HTTPX,
                    ledger=recorder.ledger,
                )
                return response

        return _RecordingClient


class CorpusReplayer(_CaptureBase):
    """Serves every intercepted request from the corpus, or fails loudly."""

    def fetch_text(self, url: str, timeout_s: int, *args: Any, **kwargs: Any) -> str:
        request = kwargs.get("request", args[0] if args else None)
        headers = _request_headers(request)
        self.ledger.note_offer(url, surface=SURFACE_FETCH_TEXT)
        entry = self.store.lookup(
            url=url, request_headers=headers, surface=SURFACE_FETCH_TEXT, ledger=self.ledger
        )
        if str(entry.get("outcome")) != "ok":
            raise_for_entry(entry)
        return self.store.body_text(entry)

    def urlopen(self, url: Any, *args: Any, **kwargs: Any) -> Any:
        request_url = str(getattr(url, "full_url", None) or url)
        method = str(getattr(url, "method", None) or "GET")
        headers = _request_headers(url)
        self.ledger.note_offer(request_url, surface=SURFACE_URLOPEN)
        entry = self.store.lookup(
            url=request_url,
            method=method,
            request_headers=headers,
            surface=SURFACE_URLOPEN,
            ledger=self.ledger,
        )
        if str(entry.get("outcome")) != "ok":
            raise_for_entry(entry)
        status = int(entry.get("statusCode") or 200)
        pairs = _entry_header_pairs(entry)
        chain = [str(item) for item in entry.get("redirectChain") or []]
        resolved = chain[-1] if chain else str(entry.get("url") or request_url)
        if status >= 400:
            # Live urlopen raises for 4xx/5xx, so replay must too.
            raise HTTPError(request_url, status, f"HTTP {status}", _ReplayHeaders(pairs), None)
        return _ReplayUrlopenResponse(
            url=request_url,
            resolved_url=resolved,
            status=status,
            headers=pairs,
            body=self.store.body_text(entry).encode("utf-8"),
        )

    def httpx_client_factory(self, real_client: Any) -> Any:
        import httpx

        replayer = self

        class _ReplayingClient:
            def __init__(self, **kwargs: Any) -> None:
                self._headers = {
                    str(name): str(value)
                    for name, value in dict(kwargs.get("headers") or {}).items()
                }
                self.is_closed = False

            def request(self, method: str, url: Any, **kwargs: Any) -> Any:
                call_headers = {
                    str(name): str(value)
                    for name, value in dict(kwargs.get("headers") or {}).items()
                }
                merged = {**self._headers, **call_headers}
                request_url = str(url)
                replayer.ledger.note_offer(request_url, surface=SURFACE_HTTPX)
                entry = replayer.store.lookup(
                    url=request_url,
                    method=str(method),
                    request_headers=merged,
                    surface=SURFACE_HTTPX,
                    ledger=replayer.ledger,
                )
                if str(entry.get("outcome")) != "ok":
                    raise_for_entry(entry, httpx_module=httpx)
                status = int(entry.get("statusCode") or 200)
                # The chain's last hop is the URL the live response reported, which is what
                # the redirect pool reads off ``response.url``.
                chain = [str(item) for item in entry.get("redirectChain") or []]
                resolved = chain[-1] if chain else str(entry.get("url") or request_url)
                request = httpx.Request(str(method).upper(), resolved)
                # A real httpx.Response, so `.url`, `.headers`, `.text` and
                # `.raise_for_status()` all behave as the caller expects.
                response = httpx.Response(
                    status_code=status,
                    headers=_entry_header_map(entry),
                    content=replayer.store.body_text(entry).encode("utf-8"),
                    request=request,
                )
                return response

            def get(self, url: Any, **kwargs: Any) -> Any:
                return self.request("GET", url, **kwargs)

            def head(self, url: Any, **kwargs: Any) -> Any:
                return self.request("HEAD", url, **kwargs)

            def post(self, url: Any, **kwargs: Any) -> Any:
                return self.request("POST", url, **kwargs)

            def close(self) -> None:
                self.is_closed = True

            def __enter__(self) -> _ReplayingClient:
                return self

            def __exit__(self, *exc_info: Any) -> None:
                self.close()

        return _ReplayingClient


__all__ = [
    "ALL_SURFACES",
    "CaptureRecorder",
    "CorpusMiss",
    "CorpusReplayer",
    "EgressBlocked",
    "EgressGuard",
    "OUT_OF_LANE_NETWORK_CALLERS",
    "install_egress_guard",
    "raise_for_entry",
]
