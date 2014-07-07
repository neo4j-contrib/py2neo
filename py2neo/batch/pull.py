#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2011-2014, Nigel Small
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


from __future__ import division, unicode_literals

from py2neo.core import Node, Rel, Path
from py2neo.batch.core import Batch, Job


class PullJob(Job):

    def __init__(self, method, entity, *segments, **kwargs):
        self.entity = entity
        uri = self.uri_for(entity, *segments, **kwargs)
        Job.__init__(self, method, uri)


class PullPropertiesJob(PullJob):

    raw_result = True

    def __init__(self, entity):
        PullJob.__init__(self, "GET", entity, "properties")


class PullNodeLabelsJob(PullJob):

    raw_result = True

    def __init__(self, node):
        PullJob.__init__(self, "GET", node, "labels")


class PullRelationshipJob(PullJob):

    raw_result = True

    def __init__(self, relationship):
        PullJob.__init__(self, "GET", relationship)


class PullBatch(Batch):

    def __init__(self, graph):
        Batch.__init__(self, graph)

    def append(self, entity):
        if isinstance(entity, Node):
            self.jobs.append(PullPropertiesJob(entity))
            self.jobs.append(PullNodeLabelsJob(entity))
        elif isinstance(entity, Rel):
            self.jobs.append(PullRelationshipJob(entity))
        elif isinstance(entity, Path):
            for relationship in entity.relationships:
                self.jobs.append(PullRelationshipJob(relationship))
        else:
            raise TypeError("Cannot pull object of type %s" % entity.__class__.__name__)

    def pull(self):
        for i, result in enumerate(self.graph.batch.submit(self)):
            job = self.jobs[i]
            if isinstance(job, PullPropertiesJob):
                job.entity.properties.replace(result.content)
            elif isinstance(job, PullNodeLabelsJob):
                job.entity.labels.replace(result.content)
            elif isinstance(job, PullRelationshipJob):
                job.entity.__class__.hydrate(result.content, job.entity)
            else:
                raise TypeError("Unsupported job type")