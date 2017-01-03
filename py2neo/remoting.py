#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2016, Nigel Small
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from py2neo.http import WebResource


class Remote(WebResource):

    _graph_service = None

    def __init__(self, uri, metadata=None):
        WebResource.__init__(self, uri)
        self._last_get_response = None
        if metadata is None:
            self._initial_metadata = None
        else:
            self._initial_metadata = dict(metadata)

    def __repr__(self):
        return "<%s uri=%r>" % (self.__class__.__name__, self.uri)

    @property
    def graph_service(self):
        """ The root service associated with this resource.
        """
        if self._graph_service is None:
            uri = self.uri
            graph_service_uri = uri[:uri.find("/", uri.find("//") + 2)] + "/"
            if graph_service_uri == uri:
                self._graph_service = self
            else:
                from py2neo.graph import GraphService
                self._graph_service = GraphService(graph_service_uri)
        return self._graph_service

    @property
    def graph(self):
        """ The parent graph of this resource.
        """
        return self.graph_service.graph

    @property
    def metadata(self):
        """ Metadata received in the last HTTP response.
        """
        if self._last_get_response is None:
            if self._initial_metadata is not None:
                return self._initial_metadata
            self.get()
        return self._last_get_response.content

    def get(self):
        self._last_get_response = super(Remote, self).get()
        return self._last_get_response


class RemoteEntity(Remote):
    """ A handle to a remote entity in a graph database.
    """

    def __init__(self, uri, metadata=None):
        super(RemoteEntity, self).__init__(uri, metadata=metadata)
        self.ref = self.uri[len(remote(self.graph).uri):]
        self._id = int(self.ref.rpartition("/")[2])

    def __repr__(self):
        return "<%s graph=%r ref=%r>" % (self.__class__.__name__,
                                         remote(self.graph).uri, self.ref)


def remote(obj):
    """ Return the remote counterpart of a local object.

    :param obj: the local object
    :return: the corresponding remote entity
    """
    try:
        return obj.__remote__
    except AttributeError:
        return None
