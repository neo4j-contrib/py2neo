from httpstream import (
    ClientError as _ClientError,
    ServerError as _ServerError,
    Response as _Response,
)
from jsonstream import assembled


class IndexTypeError(TypeError):
    pass


class ServerException(object):

    def __init__(self, data):
        self._message = str(data.get("message"))
        self._exception = str(data.get("exception"))
        self._full_name = str(data.get("fullname"))
        self._stack_trace = data.get("stacktrace")
        try:
            self._cause = ServerException(data["cause"])
        except KeyError:
            self._cause = None

    @property
    def message(self):
        return self._message

    @property
    def exception(self):
        return self._exception

    @property
    def full_name(self):
        return self._full_name

    @property
    def stack_trace(self):
        return self._stack_trace

    @property
    def cause(self):
        return self._cause


class ClientError(_ClientError):

    def __init__(self, http, uri, request, response, **kwargs):
        assert response.status // 100 == 4
        _Response.__init__(self, http, uri, request, response, **kwargs)
        if self.is_json:
            self._server_exception = ServerException(assembled(self))
            self._reason = self._server_exception.message
        else:
            self._server_exception = None
        Exception.__init__(self, self.reason)

    @property
    def message(self):
        return self._server_exception.message

    @property
    def exception(self):
        return self._server_exception.exception

    @property
    def full_name(self):
        return self._server_exception.full_name

    @property
    def stack_trace(self):
        return self._server_exception.stack_trace

    @property
    def cause(self):
        return self._server_exception.cause


class ServerError(_ServerError):

    def __init__(self, http, uri, request, response, **kwargs):
        assert response.status // 100 == 5
        _Response.__init__(self, http, uri, request, response, **kwargs)
        # TODO: check for unhandled HTML errors (on 500)
        if self.is_json:
            self._server_exception = ServerException(assembled(self))
            self._reason = self._server_exception.message
        else:
            self._server_exception = None
        Exception.__init__(self, self.reason)

    @property
    def message(self):
        return self._server_exception.message

    @property
    def exception(self):
        return self._server_exception.exception

    @property
    def full_name(self):
        return self._server_exception.full_name

    @property
    def stack_trace(self):
        return self._server_exception.stack_trace

    @property
    def cause(self):
        return self._server_exception.cause


class CypherError(Exception):

    def __init__(self, response):
        self._response = response
        Exception.__init__(self, self.message)

    @property
    def message(self):
        return self._response.message

    @property
    def exception(self):
        return self._response.exception

    @property
    def full_name(self):
        return self._response.full_name

    @property
    def stack_trace(self):
        return self._response.stack_trace

    @property
    def cause(self):
        return self._response.cause

    @property
    def request(self):
        return self._response.request

    @property
    def response(self):
        return self._response


class BatchError(Exception):

    def __init__(self, response):
        self._response = response
        Exception.__init__(self, self.message)

    @property
    def message(self):
        return self._response.message

    @property
    def exception(self):
        return self._response.exception

    @property
    def full_name(self):
        return self._response.full_name

    @property
    def stack_trace(self):
        return self._response.stack_trace

    @property
    def cause(self):
        return self._response.cause

    @property
    def request(self):
        return self._response.request

    @property
    def response(self):
        return self._response
