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
    :show-inheritance:

.. autoclass:: py2neo.rest.ResourceNotFound
    :show-inheritance:

.. autoclass:: py2neo.rest.ResourceConflict
    :show-inheritance:
