from __future__ import annotations

import ast
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADMIN_BRIDGE_API = "docs/admin-bridge-api.md"

ROUTE_HANDLER_METHODS = {
    "src/bridge/routes/get_routes.py": "GET",
    "src/bridge/routes/post_routes_admin.py": "POST",
    "src/bridge/routes/post_routes_local_data.py": "POST",
    "src/bridge/routes/post_routes_update.py": "POST",
}

EXACT = "exact"
PREFIX = "prefix"
SURFACES = {"public", "support", "internal"}
MATCH_KINDS = {EXACT, PREFIX}


@dataclass(frozen=True)
class BridgeRoute:
    method: str
    pattern: str
    match_kind: str
    handler_file: str
    surface: str
    caller_files: tuple[str, ...] = ()
    contract_doc: str = ADMIN_BRIDGE_API
    verification: str = (
        "python -m pytest tests/bridge/test_routes_get.py tests/bridge/test_routes_smoke.py -q"
    )
    doc_pattern: str = ""
    rationale: str = ""

    def key(self) -> tuple[str, str, str, str]:
        return (self.method, self.match_kind, self.pattern, self.handler_file)

    def doc_token(self) -> str:
        return self.doc_pattern or self.pattern


@dataclass(frozen=True)
class DiscoveredRoute:
    method: str
    pattern: str
    match_kind: str
    handler_file: str
    line: int

    def key(self) -> tuple[str, str, str, str]:
        return (self.method, self.match_kind, self.pattern, self.handler_file)


GET_HANDLER = "src/bridge/routes/get_routes.py"
POST_ADMIN_HANDLER = "src/bridge/routes/post_routes_admin.py"
POST_LOCAL_DATA_HANDLER = "src/bridge/routes/post_routes_local_data.py"
POST_UPDATE_HANDLER = "src/bridge/routes/post_routes_update.py"

ADMIN_DISCOVERY_CALLERS = (
    "frontend/admin/app/discovery.js",
    "frontend/admin/app/discovery/logs.js",
    "frontend/admin/app/discovery/progress.js",
    "frontend/admin/app/discovery/watch.js",
)
ADMIN_FETCHER_CALLERS = (
    "frontend/admin/app/fetcher.js",
    "frontend/admin/app/fetcher/logs.js",
    "frontend/admin/app/fetcher/report.js",
    "frontend/admin/app/fetcher/watch.js",
)
ADMIN_OPS_CALLERS = (
    "frontend/admin/app/ops/bridge-status.js",
    "frontend/admin/app/ops/health.js",
    "frontend/admin/app/ops/task-state.js",
)
ADMIN_REGISTRY_CALLERS = (
    "frontend/admin/app/registry/load.js",
    "frontend/admin/app/registry/mutations.js",
)
ADMIN_SYNC_CALLERS = ("frontend/admin/app/sync.js",)
LOCAL_DATA_CALLERS = (
    "frontend/shared/local-data/desktop/api.js",
    "frontend/shared/local-data/desktop/lifecycle.js",
    "frontend/shared/local-data/desktop/state.js",
)
PIPELINE_CALLERS = (
    "frontend/jobs/app/runtime.js",
    "frontend/jobs/app/runtime/pipeline-controller.js",
)
JOBS_BOOTSTRAP_CALLERS = (
    "frontend/jobs/app/feed.js",
    "frontend/jobs/app/runtime/boot.js",
)
STARTUP_METRIC_CALLERS = (
    "frontend/admin/data-source.js",
    "frontend/saved/app/runtime/composition.js",
)
UPDATE_CALLERS = (
    "frontend/jobs/app/desktop-update-controller.js",
    "frontend/shared/app-version.js",
)


def _route(
    method: str,
    pattern: str,
    match_kind: str,
    handler_file: str,
    surface: str,
    *,
    caller_files: tuple[str, ...] = (),
    contract_doc: str = ADMIN_BRIDGE_API,
    verification: str = "python -m pytest tests/bridge/test_routes_get.py tests/bridge/test_routes_smoke.py -q",
    doc_pattern: str = "",
    rationale: str = "",
) -> BridgeRoute:
    return BridgeRoute(
        method=method,
        pattern=pattern,
        match_kind=match_kind,
        handler_file=handler_file,
        surface=surface,
        caller_files=caller_files,
        contract_doc=contract_doc,
        verification=verification,
        doc_pattern=doc_pattern,
        rationale=rationale,
    )


BRIDGE_ROUTES: tuple[BridgeRoute, ...] = (
    _route(
        "GET",
        "/discovery/report",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_DISCOVERY_CALLERS,
    ),
    _route(
        "GET",
        "/discovery/candidates",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=ADMIN_DISCOVERY_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/session",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/profiles",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/saved-jobs",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/saved-job-keys",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/attachments",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/attachments/content",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/backup/export-file",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/activity",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "GET",
        "/desktop-local-data/startup-metrics",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=STARTUP_METRIC_CALLERS,
    ),
    _route("GET", "/app/update-status", EXACT, GET_HANDLER, "public", caller_files=UPDATE_CALLERS),
    _route(
        "GET", "/registry/active", EXACT, GET_HANDLER, "public", caller_files=ADMIN_REGISTRY_CALLERS
    ),
    _route(
        "GET",
        "/registry/pending",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "GET",
        "/registry/rejected",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "GET",
        "/registry/sources",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "GET", "/discovery/log", EXACT, GET_HANDLER, "support", caller_files=ADMIN_DISCOVERY_CALLERS
    ),
    _route(
        "GET", "/fetcher/log", EXACT, GET_HANDLER, "support", caller_files=ADMIN_FETCHER_CALLERS
    ),
    _route(
        "GET",
        "/registry/summary",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "GET",
        "/ops/health",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS + ("frontend/shared/admin-bridge-button.js",),
    ),
    _route(
        "GET",
        "/ops/dashboard-health",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route("GET", "/ops/history", EXACT, GET_HANDLER, "support", caller_files=ADMIN_OPS_CALLERS),
    _route(
        "GET",
        "/discovery/config",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=ADMIN_DISCOVERY_CALLERS,
    ),
    _route(
        "GET",
        "/ops/task-state",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS + PIPELINE_CALLERS,
    ),
    _route(
        "GET",
        "/ops/task-live/",
        PREFIX,
        GET_HANDLER,
        "support",
        caller_files=("frontend/admin/app/live-task.js",),
    ),
    _route(
        "GET",
        "/ops/fetcher-metrics",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_FETCHER_CALLERS,
    ),
    _route(
        "GET",
        "/ops/perf-counters",
        EXACT,
        GET_HANDLER,
        "internal",
        contract_doc="",
        rationale="Internal diagnostics snapshot for route timing and perf-counter tests.",
    ),
    _route(
        "GET", "/ops/storage-metrics", EXACT, GET_HANDLER, "support", caller_files=ADMIN_OPS_CALLERS
    ),
    _route(
        "GET", "/ops/storage-health", EXACT, GET_HANDLER, "support", caller_files=ADMIN_OPS_CALLERS
    ),
    _route(
        "GET",
        "/ops/discovery-audit-artifacts",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route(
        "GET",
        "/ops/fetch-report/sources",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_FETCHER_CALLERS,
    ),
    _route(
        "GET",
        "/ops/fetch-report",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_FETCHER_CALLERS,
    ),
    _route(
        "GET",
        "/source-policy/recommendations",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route(
        "GET",
        "/registry/conflicts",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route("GET", "/sync/status", EXACT, GET_HANDLER, "public", caller_files=ADMIN_SYNC_CALLERS),
    _route(
        "GET",
        "/tasks/run-jobs-pipeline-status",
        EXACT,
        GET_HANDLER,
        "public",
        caller_files=PIPELINE_CALLERS,
    ),
    _route(
        "GET",
        "/tasks/jobs-pipeline-schedule",
        EXACT,
        GET_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
        verification="python -m pytest tests/bridge/test_pipeline_schedule_routes.py -q",
    ),
    _route(
        "POST",
        "/dedup/review-action",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route(
        "POST",
        "/source-policy/review-action",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route(
        "POST",
        "/source-policy/migration-link-action",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route(
        "POST",
        "/sources/manual",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/discovery/check-source",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_DISCOVERY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/conflicts/check-sources",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/approve",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/reject",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/rollback",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/demote-active",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/conflicts/auto-demote-safe",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/restore-rejected",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/restore-deleted",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/registry/delete",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_REGISTRY_CALLERS,
    ),
    _route(
        "POST",
        "/tasks/run-discovery",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_DISCOVERY_CALLERS,
    ),
    _route(
        "POST",
        "/tasks/run-jobs-pipeline",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=PIPELINE_CALLERS,
    ),
    _route(
        "POST",
        "/tasks/jobs-pipeline-schedule",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
        verification="python -m pytest tests/bridge/test_pipeline_schedule_routes.py -q",
    ),
    _route(
        "POST",
        "/tasks/abort",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=[*PIPELINE_CALLERS, *ADMIN_OPS_CALLERS],
    ),
    _route(
        "POST",
        "/tasks/run-jobs-bootstrap",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=JOBS_BOOTSTRAP_CALLERS,
        verification="python -m pytest tests/bridge/test_routes_post.py "
        "tests/bridge/test_task_launch_bootstrap.py -q",
    ),
    _route(
        "POST",
        "/tasks/run-sync-pull",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_SYNC_CALLERS,
    ),
    _route(
        "POST",
        "/tasks/run-sync-push",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_SYNC_CALLERS,
    ),
    _route(
        "POST",
        "/tasks/run-fetcher",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_FETCHER_CALLERS,
    ),
    _route(
        "POST",
        "/discovery/config",
        EXACT,
        POST_ADMIN_HANDLER,
        "public",
        caller_files=ADMIN_DISCOVERY_CALLERS,
    ),
    _route(
        "POST",
        "/ops/alerts/ack",
        EXACT,
        POST_ADMIN_HANDLER,
        "support",
        caller_files=ADMIN_OPS_CALLERS,
    ),
    _route(
        "POST", "/sync/config", EXACT, POST_ADMIN_HANDLER, "public", caller_files=ADMIN_SYNC_CALLERS
    ),
    _route(
        "POST", "/sync/test", EXACT, POST_ADMIN_HANDLER, "public", caller_files=ADMIN_SYNC_CALLERS
    ),
    _route(
        "POST", "/sync/pull", EXACT, POST_ADMIN_HANDLER, "public", caller_files=ADMIN_SYNC_CALLERS
    ),
    _route(
        "POST", "/sync/push", EXACT, POST_ADMIN_HANDLER, "public", caller_files=ADMIN_SYNC_CALLERS
    ),
    _route(
        "POST",
        "/desktop-local-data/sign-in",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/sign-out",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/saved-jobs/save",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/saved-jobs/remove",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/saved-jobs/status",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/saved-jobs/tracking",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/saved-jobs/notes",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/attachments/add",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/attachments/delete",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/backup/export",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/backup/import",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/admin/overview",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "support",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/admin/wipe",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "support",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/app/desktop-session-lifecycle",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/startup-metric",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "support",
        caller_files=STARTUP_METRIC_CALLERS,
    ),
    _route(
        "POST",
        "/desktop-local-data/open-url",
        EXACT,
        POST_LOCAL_DATA_HANDLER,
        "public",
        caller_files=LOCAL_DATA_CALLERS,
    ),
    _route(
        "POST",
        "/app/check-for-update",
        EXACT,
        POST_UPDATE_HANDLER,
        "public",
        caller_files=UPDATE_CALLERS,
    ),
    _route(
        "POST",
        "/app/download-update",
        EXACT,
        POST_UPDATE_HANDLER,
        "public",
        caller_files=UPDATE_CALLERS,
    ),
    _route(
        "POST",
        "/app/install-update",
        EXACT,
        POST_UPDATE_HANDLER,
        "public",
        caller_files=UPDATE_CALLERS,
    ),
)


def bridge_routes() -> tuple[BridgeRoute, ...]:
    return BRIDGE_ROUTES


def _repo_root(repo_root: Path | None) -> Path:
    return ROOT if repo_root is None else repo_root


def _is_path_name(node: ast.AST) -> bool:
    return isinstance(node, ast.Name) and node.id == "path"


def _string_literal(node: ast.AST) -> str | None:
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value.startswith("/")
    ):
        return node.value
    return None


def _string_literals(node: ast.AST) -> tuple[str, ...]:
    if not isinstance(node, ast.Set | ast.Tuple | ast.List):
        return ()
    return tuple(value for element in node.elts if (value := _string_literal(element)) is not None)


def _route_sort_key(route: DiscoveredRoute) -> tuple[str, int, str, str]:
    return (route.handler_file, route.line, route.match_kind, route.pattern)


def _discover_routes_in_tree(
    tree: ast.AST, *, method: str, handler_file: str
) -> tuple[DiscoveredRoute, ...]:
    routes: list[DiscoveredRoute] = []

    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            expressions = [node.left, *node.comparators]
            for index, op in enumerate(node.ops):
                left = expressions[index]
                right = expressions[index + 1]
                if isinstance(op, ast.Eq):
                    for candidate in (right, left):
                        if candidate is right and not _is_path_name(left):
                            continue
                        if candidate is left and not _is_path_name(right):
                            continue
                        pattern = _string_literal(candidate)
                        if pattern is not None:
                            routes.append(
                                DiscoveredRoute(method, pattern, EXACT, handler_file, node.lineno)
                            )
                if isinstance(op, ast.In) and _is_path_name(left):
                    for pattern in _string_literals(right):
                        routes.append(
                            DiscoveredRoute(method, pattern, EXACT, handler_file, node.lineno)
                        )

        if isinstance(node, ast.Call):
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "startswith"
                and _is_path_name(func.value)
                and node.args
            ):
                pattern = _string_literal(node.args[0])
                if pattern is not None:
                    routes.append(
                        DiscoveredRoute(method, pattern, PREFIX, handler_file, node.lineno)
                    )

    return tuple(sorted(routes, key=_route_sort_key))


def discover_bridge_routes(repo_root: Path | None = None) -> tuple[DiscoveredRoute, ...]:
    root = _repo_root(repo_root)
    discovered: list[DiscoveredRoute] = []
    for handler_file, method in ROUTE_HANDLER_METHODS.items():
        source_path = root / handler_file
        tree = ast.parse(source_path.read_text(encoding="utf-8"), filename=str(source_path))
        discovered.extend(_discover_routes_in_tree(tree, method=method, handler_file=handler_file))
    return tuple(sorted(discovered, key=_route_sort_key))


def _duplicates(keys: list[tuple[str, str, str, str]]) -> list[tuple[str, str, str, str]]:
    counts = Counter(keys)
    return sorted(key for key, count in counts.items() if count > 1)


def _format_key(key: tuple[str, str, str, str]) -> str:
    method, match_kind, pattern, handler_file = key
    return f"{method} {match_kind} {pattern} in {handler_file}"


def _rel_path_valid(rel_path: str) -> bool:
    path = Path(rel_path)
    return bool(rel_path) and not path.is_absolute() and ".." not in path.parts


def _doc_mentions_route(route: BridgeRoute, doc_text: str) -> bool:
    route_pattern = re.escape(route.doc_token())
    row_pattern = rf"\|\s*{route.method}\s*\|\s*`{route_pattern}(?=[`?<])"
    return re.search(row_pattern, doc_text) is not None


def _validate_inventory_entries(repo_root: Path) -> list[str]:
    failures: list[str] = []
    inventory_keys = [route.key() for route in BRIDGE_ROUTES]
    for key in _duplicates(inventory_keys):
        failures.append(f"duplicate bridge route inventory entry: {_format_key(key)}")

    doc_cache: dict[str, str] = {}
    for route in BRIDGE_ROUTES:
        label = _format_key(route.key())
        if route.method not in {"GET", "POST"}:
            failures.append(f"{label} has unsupported method `{route.method}`.")
        if route.match_kind not in MATCH_KINDS:
            failures.append(f"{label} has unsupported match kind `{route.match_kind}`.")
        if route.surface not in SURFACES:
            failures.append(f"{label} has unsupported surface `{route.surface}`.")
        if not route.pattern.startswith("/"):
            failures.append(f"{label} pattern must start with `/`.")
        if not _rel_path_valid(route.handler_file):
            failures.append(f"{label} handler file must be repo-relative.")
        elif not (repo_root / route.handler_file).is_file():
            failures.append(f"{label} handler file is missing.")

        for caller_file in route.caller_files:
            if not _rel_path_valid(caller_file):
                failures.append(f"{label} caller file must be repo-relative: {caller_file}")
            elif not (repo_root / caller_file).is_file():
                failures.append(f"{label} caller file is missing: {caller_file}")

        if route.surface == "internal":
            if not route.rationale.strip():
                failures.append(f"{label} internal route must include a rationale.")
            continue

        if not route.contract_doc.strip():
            failures.append(f"{label} public/support route must list a contract doc.")
            continue
        if not _rel_path_valid(route.contract_doc):
            failures.append(f"{label} contract doc must be repo-relative.")
            continue
        contract_path = repo_root / route.contract_doc
        if not contract_path.is_file():
            failures.append(f"{label} contract doc is missing: {route.contract_doc}")
            continue
        doc_text = doc_cache.setdefault(
            route.contract_doc, contract_path.read_text(encoding="utf-8")
        )
        if not _doc_mentions_route(route, doc_text):
            failures.append(f"{label} is missing from {route.contract_doc}.")

    return failures


def check_bridge_route_inventory(repo_root: Path | None = None) -> list[str]:
    root = _repo_root(repo_root)
    failures = _validate_inventory_entries(root)
    inventory_by_key = {route.key(): route for route in BRIDGE_ROUTES}
    discovered = discover_bridge_routes(root)
    discovered_by_key = {route.key(): route for route in discovered}

    for route in discovered:
        if route.key() not in inventory_by_key:
            failures.append(
                f"{_format_key(route.key())}:{route.line} is missing from bridge route inventory."
            )

    for key in sorted(inventory_by_key):
        if key not in discovered_by_key:
            failures.append(
                f"{_format_key(key)} is listed in bridge route inventory but was not found."
            )

    return sorted(failures)
