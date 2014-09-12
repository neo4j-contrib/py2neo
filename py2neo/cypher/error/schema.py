
from py2neo.cypher.error.core import ClientError, DatabaseError


class ConstraintAlreadyExists(ClientError):
    """ Unable to perform operation because it would clash with a
    pre-existing constraint.
    """


class ConstraintVerificationFailure(ClientError):
    """ Unable to create constraint because data that exists in the
    database violates it.
    """


class ConstraintViolation(ClientError):
    """ A constraint imposed by the database was violated.
    """


class IllegalTokenName(ClientError):
    """ A token name, such as a label, relationship type or property
    key, used is not valid. Tokens cannot be empty strings and cannot
    be null.
    """


class IndexAlreadyExists(ClientError):
    """ Unable to perform operation because it would clash with a
    pre-existing index.
    """


class IndexBelongsToConstraint(ClientError):
    """ A requested operation can not be performed on the specified
    index because the index is part of a constraint. If you want to
    drop the index, for instance, you must drop the constraint.
    """


class LabelLimitReached(ClientError):
    """ The maximum number of labels supported has been reached, no
    more labels can be created.
    """


class NoSuchConstraint(ClientError):
    """ The request (directly or indirectly) referred to a constraint
    that does not exist.
    """


class NoSuchIndex(ClientError):
    """ The request (directly or indirectly) referred to an index that
    does not exist.
    """


class ConstraintCreationFailure(DatabaseError):
    """ Creating a requested constraint failed.
    """


class ConstraintDropFailure(DatabaseError):
    """ The database failed to drop a requested constraint.
    """


class IndexCreationFailure(DatabaseError):
    """ Failed to create an index.
    """


class IndexDropFailure(DatabaseError):
    """ The database failed to drop a requested index.
    """


class NoSuchLabel(DatabaseError):
    """ The request accessed a label that did not exist.
    """


class NoSuchPropertyKey(DatabaseError):
    """ The request accessed a property that does not exist.
    """


class NoSuchRelationshipType(DatabaseError):
    """ The request accessed a relationship type that does not exist.
    """


class NoSuchSchemaRule(DatabaseError):
    """ The request referred to a schema rule that does not exist.
    """
