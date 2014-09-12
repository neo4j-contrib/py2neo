

from py2neo.cypher.error.core import ClientError, DatabaseError


class ReadOnly(ClientError):
    """ This is a read only database, writing or modifying the database
    is not allowed.
    """


class CorruptSchemaRule(DatabaseError):
    """ A malformed schema rule was encountered. Please contact your
    support representative.
    """


class FailedIndex(DatabaseError):
    """ The request (directly or indirectly) referred to an index that
    is in a failed state. The index needs to be dropped and recreated
    manually.
    """


class UnknownFailure(DatabaseError):
    """ An unknown failure occurred.
    """
