

import os
from shutil import rmtree


class GraphStore(object):

    # TODO: instances

    @classmethod
    def for_server(cls, server):
        # TODO: actually sniff config files for the true path
        return GraphStore(os.path.join(server.home, "data", "graph.db"))

    def __init__(self, path):
        self.path = path

    @property
    def locked(self):
        return os.path.isfile(os.path.join(self.path, "lock"))

    def drop(self, force=False):
        if not force and self.locked:
            raise RuntimeError("Refusing to drop database store while in use")
        else:
            rmtree(self.path, ignore_errors=True)
