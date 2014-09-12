

from py2neo.cypher.error.core import ClientError


class Invalid(ClientError):
    """ The client provided an invalid request.
    """


class InvalidFormat(ClientError):
    """ The client provided a request that was missing required fields,
    or had values that are not allowed.
    """
