class ApolloError(Exception):
    """Raised when config can not be fetched from Apollo."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
