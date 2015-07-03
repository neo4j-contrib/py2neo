#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2015, Nigel Small
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


from py2neo.batch import Batch, CypherJob
from py2neo.batch.push import PushPropertiesJob, PushNodeLabelsJob
from py2neo.cypher import CreateUniqueConstraint, CreateNode, MergeNode


class SaveBatch(Batch):
    """ A batch of save jobs.
    """

    def __init__(self, graph):
        Batch.__init__(self, graph)
        self.objects = {}

    def append(self, obj):
        labels = set(getattr(obj, "__labels__", []))
        label = getattr(obj, "__pl__", obj.__class__.__name__)
        labels.add(label)
        properties = {key: value for key, value in obj.__dict__.items() if not key.startswith("_")}
        try:
            uri = getattr(obj, "__uri__")
        except AttributeError:
            # If the object has no uri, determine whether to create or merge
            # base on the existence of a __pk__ attribute
            job_id = len(self.jobs)
            try:
                key = getattr(obj, "__pk__")
            except AttributeError:
                task = CreateNode(*labels, **properties).with_return()
            else:
                value = getattr(obj, key, None)
                task = MergeNode(label, key, value).set(*labels, **properties).with_return()
            self.jobs.append(CypherJob(task.statement, task.parameters))
            self.objects[job_id] = obj
        else:
            # If the obj already has a __uri__, simply push it
            self.jobs.append(PushPropertiesJob(uri, properties))
            self.jobs.append(PushNodeLabelsJob(uri, labels))
        # TODO: save rels

    def save(self):
        for result in self.graph.batch.submit(self):
            try:
                self.objects[result.job_id].__uri__ = result.content.uri.string
            except AttributeError:
                pass
        self.objects.clear()


class Store(object):

    def __init__(self, graph):
        self.graph = graph

    def set_unique(self, label, key):
        self.graph.cypher.execute(CreateUniqueConstraint(label, key))

    def save(self, *objects):
        """ Attempt to save all objects into the graph. Each object may be
        created, merged or pushed, depending on its status and metadata.

        :param objects: objects to save
        :return:
        """
        batch = SaveBatch(self.graph)
        for obj in objects:
            batch.append(obj)
        batch.save()

    def relate(self, subj, rel_type, obj, properties=None):
        """ Define a relationship between `subj` and `obj` of type `rel_type`.
        This is a local operation only: nothing is saved to the database until
        a save method is called. Relationship properties may optionally be
        specified.

        :param subj: the object bound to the start of the relationship
        :param rel_type: the relationship type
        :param obj: the object bound to the end of the relationship
        :param properties: properties attached to the relationship (optional)
        """
        if not hasattr(subj, "__rel__"):
            subj.__rel__ = {}
        if rel_type not in subj.__rel__:
            subj.__rel__[rel_type] = []
        subj.__rel__[rel_type].append((properties or {}, obj))
