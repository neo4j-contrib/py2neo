
from py2neo.cypher.error.core import ClientError, DatabaseError, TransientError


class ConcurrentRequest(ClientError):
    """ There were concurrent requests accessing the same transaction,
    which is not allowed.
    """


class EventHandlerThrewException(ClientError):
    """ A transaction event handler threw an exception. The transaction
    will be rolled back.
    """


class InvalidType(ClientError):
    """ The transaction is of the wrong type to service the request.
    For instance, a transaction that has had schema modifications
    performed in it cannot be used to subsequently perform data
    operations, and vice versa.
    """


class UnknownId(ClientError):
    """ The request referred to a transaction that does not exist.
    """


class CouldNotBegin(DatabaseError):
    """ The database was unable to start the transaction.
    """


class CouldNotCommit(DatabaseError):
    """ The database was unable to commit the transaction.
    """


class CouldNotRollback(DatabaseError):
    """ The database was unable to roll back the transaction.
    """


class ReleaseLocksFailed(DatabaseError):
    """ The transaction was unable to release one or more of its locks.
    """


class AcquireLockTimeout(TransientError):
    """ The transaction was unable to acquire a lock, for instance due
    to a timeout or the transaction thread being interrupted.
    """


class DeadlockDetected(TransientError):
    """ This transaction, and at least one more transaction, has
    acquired locks in a way that it will wait indefinitely, and the
    database has aborted it. Retrying this transaction will most likely
    be successful.
    """
