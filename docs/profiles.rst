*******************
Connection profiles
*******************

.. module:: py2neo
    :noindex:

Most py2neo applications will make use of a class such as :class:`.Graph` or :class:`.GraphService` as the backbone through which Neo4j access is made.
On construction, these classes generally accept a `profile` plus additional individual settings, that together specify how and where to connect.

    >>> from py2neo import Graph
    >>> g = Graph("neo4j+s://graph.example.com:7687", auth=("alice", "123456"))

The profile can take one of several forms, the simplest of which is a URI.
Alternatively, a :class:`.ConnectionProfile` or :class:`.ServiceProfile` object can be supplied instead.
These forms are all semantically equivalent, and provide access to the same range of options.

The additional settings allow certain parts of the profile to be overridden individually, and these will therefore be applied after the profile has been parsed.

If no arguments are provided to a :class:`.Graph` constructor, a default :class:`.ServiceProfile` will be assumed.
This default describes an unsecured `bolt <https://7687.org>`_ connection to localhost on the default port, and uses ``neo4j`` and ``password`` as the user name and password respectively.

The routing option is available wherever the profile describes connectivity to a full Neo4j service, i.e. for a :class:`.ServiceProfile` but not for a :class:`.ConnectionProfile`.
To ensure backward compatibility with older versions of Neo4j, as well as with standalone deployments, routing is not enabled for the default profile.


Environment variables
=====================

Profile defaults can be adjusted through setting environment variables made available to the Python environment.
The following variables are available:

.. envvar :: NEO4J_URI

.. envvar :: NEO4J_AUTH

.. envvar :: NEO4J_SECURE

.. envvar :: NEO4J_VERIFY


Profile URIs
============

The general format of a profile URI is ``<scheme>://[<user>[:<password>]@]<host>[:<port>]``.

Supported URI schemes are:

- ``neo4j`` - Bolt with routing (unsecured)
- ``neo4j+s`` - Bolt with routing (secured with full certificate checks)
- ``neo4j+ssc`` - Bolt with routing (secured with no certificate checks)
- ``bolt`` - Bolt direct (unsecured)
- ``bolt+s`` - Bolt direct (secured with full certificate checks)
- ``bolt+ssc`` - Bolt direct (secured with no certificate checks)
- ``http`` - HTTP direct (unsecured)
- ``https`` - HTTP direct (secured with full certificate checks)
- ``http+s`` - HTTP direct (secured with full certificate checks)
- ``http+ssc`` - HTTP direct (secured with no certificate checks)


Individual settings
===================

Any of the values below can be set as individual overrides.
These are applied in an order from broadest to finest resolution;
for example, ``auth`` would be applied before ``user`` and ``password``.

.. describe:: uri

    A full profile URI can be passed as a keyword argument.
    This is parsed identically to a URI passed into the `profile` argument.

.. describe:: scheme

    Use a specific URI scheme.

.. describe:: protocol

    The name of the protocol to use for communication.
    This can be either ``'bolt'`` or ``'http'`` and is not the same as the URI scheme, as it does not include security or verification indicators.

.. describe:: secure

    Flag to indicate that a secure connection should be used.
    Connections are secured with TLS, using Python's built-in ``ssl`` module.

.. describe:: verify

    Flag to indicate that the server certificate should be fully verified.
    This applies only if the connection is secure.

.. describe:: address

    Either a tuple (e.g. ``('localhost', 7687)``), an :class:`.Address` object, or a string (e.g. ``'localhost:7687'``)

.. describe:: host

    Database server host name.

.. describe:: port

    Database server port.

.. describe:: port_number

    Database server port number.

.. describe:: auth

    Full authentication details, comprising both user name and password.
    This can be either a 2-tuple (e.g. ``('user', 'password')``), or a string (e.g. ``'user:password'``).

.. describe:: user

    Name of the user to authenticate as.

.. describe:: password

    Password to use for authentication.

.. describe:: routing

    Flag to indicate that connections should be routed across multiple servers, whenever available.
    This keyword is only available with service profiles, not simple connection profiles.


Profile objects
===============

.. autoclass:: py2neo.ConnectionProfile
    :members:

.. autoclass:: py2neo.ServiceProfile
    :members:
    :show-inheritance:
