"""
Core data contracts and validation for Baluffo.
Runtime validation at pipeline/bridge boundaries; see docs/DATA_CONTRACT.md.
"""

from src.core.contracts import validate_canonical_jobs_payload

__all__ = ["validate_canonical_jobs_payload"]
