"""Discovery service discovery service config.

AI boundary owns: bridge-owned discovery task launch, config persistence, and auto-sync watch behavior.
AI boundary implement in: this discovery_service_config.py leaf.
AI boundary search before contracts: discovery routes, task launch API, source discovery config, and admin discovery frontend callers.
AI boundary verify: `npm run lint:repo-guardrails` plus focused discovery service tests.
"""

from __future__ import annotations

from typing import Any

from src.bridge.discovery_service_core import DiscoveryServiceState


class DiscoveryServiceConfigMixin(DiscoveryServiceState):
    @staticmethod
    def _normalize_discovery_settings(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        data = payload if isinstance(payload, dict) else {}
        raw = data.get("autoApproveHealthyPendingOnComplete", True)
        if isinstance(raw, bool):
            enabled = raw
        else:
            enabled = str(raw or "").strip().lower() not in {"", "0", "false", "no", "off"}
        return {"autoApproveHealthyPendingOnComplete": bool(enabled)}

    def load_saved_discovery_settings(self) -> dict[str, Any]:
        raw = self._deps.load_json_object(self._paths.settings, {})
        if isinstance(raw, dict) and "autoApproveHealthyPendingOnComplete" in raw:
            return self._normalize_discovery_settings(raw)
        return {}

    def get_saved_discovery_config_payload(self) -> dict[str, Any]:
        settings = self.load_saved_discovery_settings()
        if "autoApproveHealthyPendingOnComplete" in settings:
            return {
                "autoApproveHealthyPendingOnComplete": bool(
                    settings.get("autoApproveHealthyPendingOnComplete")
                )
            }
        return self._normalize_discovery_settings({})

    def get_discovery_config_payload(self) -> dict[str, Any]:
        return {
            "ok": True,
            "savedConfig": self.get_saved_discovery_config_payload(),
        }

    def update_saved_discovery_settings(self, payload: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_discovery_settings(payload)
        self._deps.save_json_atomic(self._paths.settings, normalized)
        return normalized
