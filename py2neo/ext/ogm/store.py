
from py2neo.cypher.lang import cypher_escape
from py2neo.batch.core import Batch, CypherJob
from py2neo.core import Bindable


class Store(object):

    def __init__(self, graph):
        self.graph = graph

    def save(self, *objects):
        batch = Batch(self.graph)
        for obj in objects:
            class_name = obj.__class__.__name__
            if not isinstance(obj, Bindable):
                obj.__class__ = type(class_name, (obj.__class__, Bindable), {})
            primary_label = getattr(obj, "__primarylabel__", class_name)
            primary_key = getattr(obj, "__primarykey__", None)
            if primary_key:
                job = CypherJob("MERGE (n:%s {%s:{v}}) RETURN n" % (
                    cypher_escape(primary_label), cypher_escape(primary_key)
                ), {"v": getattr(obj, primary_key, None)})
            else:
                job = CypherJob("MERGE (n:%s) RETURN n" % cypher_escape(primary_label))
            batch.append(job)
        return self.graph.batch.submit(batch)
