"""Runtime state containers for the admin bridge server.

Most operational state has already been extracted into service modules under
`src.bridge.*_service`. This module exists as a home for any remaining
HTTP-server-adjacent mutable state that should not live in `src.admin_bridge`.
"""

from __future__ import annotations

