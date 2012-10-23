:mod:`rest` Module
===================

.. automodule:: py2neo.rest

Resources
---------

All REST web service objects inherit from the base :py:class:`Resource` class.

.. autoclass:: py2neo.rest.Resource
    :members: __uri__, __metadata__, refresh

Errors
------

.. autoclass:: py2neo.rest.BadRequest
    :members: exception, message, stacktrace, id
    :show-inheritance:

.. autoclass:: py2neo.rest.ResourceNotFound
    :members: uri, id
    :show-inheritance:

.. autoclass:: py2neo.rest.ResourceConflict
    :members: uri, id
    :show-inheritance:
