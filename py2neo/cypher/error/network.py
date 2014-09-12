

from py2neo.cypher.error.core import TransientError


class UnknownFailure(TransientError):
    """ An unknown network failure occurred, a retry may resolve the
    issue.
    """
