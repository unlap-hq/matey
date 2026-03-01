from __future__ import annotations


class MateyError(Exception):
    """Base exception for matey errors."""


class ConfigError(MateyError):
    """Invalid or missing configuration."""


class TargetSelectionError(MateyError):
    """Invalid target selection for the current config."""


class PathResolutionError(MateyError):
    """Failed to derive migrations/schema paths."""


class URLResolutionError(MateyError):
    """Failed to resolve the database URL for a target."""


class ScratchProvisionError(MateyError):
    """Failed to provision scratch infrastructure."""


class SchemaValidationError(MateyError):
    """Schema validation failed before diff evaluation."""


class LockfileError(MateyError):
    """Lockfile/checkpoint integrity validation failed."""
