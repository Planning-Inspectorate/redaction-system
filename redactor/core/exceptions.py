class InvalidRedactionConfigException(Exception):  # pragma: no cover
    pass

class UnprocessedRedactionResultException(Exception):  # pragma: no cover
    pass


class DuplicateFileProcessorNameException(Exception):  # pragma: no cover
    pass


class FileProcessorNameNotFoundException(Exception):  # pragma: no cover
    pass


class NonEnglishContentException(Exception):  # pragma: no cover
    """Raised when a document is detected as non-English or insufficient English content"""
    pass


class IncorrectRedactionConfigClassException(Exception):  # pragma: no cover
    pass


class DuplicateRedactorNameException(Exception):  # pragma: no cover
    pass


class RedactorNameNotFoundException(Exception):  # pragma: no cover
    pass