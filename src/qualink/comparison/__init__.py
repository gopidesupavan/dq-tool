"""Comparison module: cross-dataset checks via DataFusion (Deequ-aligned)."""

from .referential_integrity import ReferentialIntegrity
from .row_count_match import RowCountMatch
from .schema_match import SchemaMatch

__all__ = ["ReferentialIntegrity", "RowCountMatch", "SchemaMatch"]
