"""Snapshot restoration errors."""


class SnapshotError(ValueError):
    """A snapshot could not be restored.

    Raising (rather than mutating and returning) guarantees that no partial
    recovery state is ever observable: the target replayer stays untouched.
    """

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
