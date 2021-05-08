# -*- coding: utf-8 -*-


class RQDataError(Exception):
    rqerrno = 1
    DEFAULT_MSG = "default rqdata error"

    def __str__(self):
        s = super(Exception, self).__str__()
        if s:
            return s
        return self.DEFAULT_MSG

    def __repr__(self):
        return "{}: {}".format(self.__class__.__name__, self.__str__())


class AuthenticationFailed(RQDataError):
    rqerrno = 2
    DEFAULT_MSG = "authentication failed."


class ErrorFromBackend(RQDataError):
    rqerrno = 3
    DEFAULT_MSG = "raise error from backend"
    pass


class PermissionDenied(ErrorFromBackend):
    rqerrno = 4
    DEFAULT_MSG = "permission denied"


class NoSuchService(ErrorFromBackend):
    rqerrno = 7
    DEFAULT_MSG = "Can't found the request service"


class QuotaExceeded(ErrorFromBackend):
    rqerrno = 5
    DEFAULT_MSG = "quota exceeded"


class MarketNotSupportError(ErrorFromBackend):
    rqerrno = 6
    DEFAULT_MSG = "market not supported yet."


class InternalError(RQDataError):
    rqerrno = -1
    DEFAULT_MSG = "Server raised an error and can't handle it."


class GatewayError(InternalError):
    rqerrno = -2
    DEFAULT_MSG = "Can't communicate with gateway."


class BadRequest(ErrorFromBackend):
    rqerrno = 400
    DEFAULT_MSG = "Bad Request Content"


class OverwriteWarning(Warning):
    pass


_ERROR_MAP = {}


def __go(lcs):
    global _ERROR_MAP
    _ERROR_MAP = {v.rqerrno: v for k, v in lcs.items() if hasattr(v, "rqerrno")}


__go(locals())
del __go


def get_error(errorno):
    return _ERROR_MAP.get(errorno, RQDataError)


__all__ = tuple(i.__name__ for i in _ERROR_MAP.values()) + ("get_error",)
