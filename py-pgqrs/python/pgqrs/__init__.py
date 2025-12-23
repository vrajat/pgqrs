from . import _pgqrs
from ._pgqrs import *  # noqa: F403

__doc__ = _pgqrs.__doc__
if hasattr(_pgqrs, "__all__"):
    __all__ = _pgqrs.__all__
