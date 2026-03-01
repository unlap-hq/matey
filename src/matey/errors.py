from __future__ import annotations


class MateyError(Exception):
    """Base class for all typed matey errors."""


class ConfigError(MateyError):
    pass


class TargetSelectionError(MateyError):
    pass


class LockfileError(MateyError):
    pass


class ReplayError(MateyError):
    pass


class SchemaMismatchError(MateyError):
    pass


class LiveDriftError(MateyError):
    pass


class LiveHistoryMismatchError(MateyError):
    pass


class ArtifactTransactionError(MateyError):
    pass


class ArtifactRecoveryError(MateyError):
    pass


class BigQueryPreflightError(MateyError):
    pass


class CheckpointIntegrityError(MateyError):
    pass


class EngineInferenceError(MateyError):
    pass


class TargetIdentityError(MateyError):
    pass


class ExternalCommandError(MateyError):
    pass


class CliUsageError(MateyError):
    pass
