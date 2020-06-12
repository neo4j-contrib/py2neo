#!/usr/bin/env python
# -*- encoding: utf-8 -*-

# Copyright 2011-2020, Nigel Small
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


from logging import getLogger


log = getLogger(__name__)


def resolve_image(image):
    """ Resolve an informal image tag into a full Docker image tag. Any tag
    available on Docker Hub for Neo4j can be used, and if no 'neo4j:' prefix
    exists, this will be added automatically. The default edition is
    Community, unless a cluster is being created in which case Enterprise
    edition is selected instead. Explicit selection of Enterprise edition can
    be made by adding an '-enterprise' suffix to the image tag.

    If a 'file:' URI is passed in here instead of an image tag, the Docker
    image will be loaded from that file instead.

    Examples of valid tags:
    - 3.4.6
    - neo4j:3.4.6
    - latest
    - file:/home/me/image.tar

    """
    resolved = image
    if resolved.startswith("file:"):
        return load_image_from_file(resolved[5:])
    if ":" not in resolved:
        resolved = "neo4j:" + image
    return resolved


def load_image_from_file(name):
    from docker import DockerClient
    docker = DockerClient.from_env(version="auto")
    with open(name, "rb") as f:
        images = docker.images.load(f.read())
        image = images[0]
        return image.tags[0]
