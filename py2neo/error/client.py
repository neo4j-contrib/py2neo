

""" Client Errors
"""

class BindError(Exception):
    """ Raised when a local graph entity is not or cannot be bound to a remote graph entity.
    """


class JoinError(Exception):
    """ Raised when two graph entities cannot be joined together.
    """
