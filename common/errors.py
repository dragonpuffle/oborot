"""Errors shared at application and infrastructure seams."""


class ValidationError(ValueError):
    pass


class InvalidEvent(ValidationError):
    pass


class DependencyUnavailable(Exception):
    pass
