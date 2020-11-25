from uuid import uuid4
import logging


log = logging.getLogger(__name__)


class Relationship(object):

    TYPE = None

    def __init__(self, start_node_labels, end_node_labels, start_node_properties,
                 end_node_properties, properties):

        self.start_node_labels = start_node_labels
        self.end_node_labels = end_node_labels
        self.start_node_properties = start_node_properties
        self.end_node_properties = end_node_properties
        self.properties = properties
        self.object_type = self.TYPE

    def to_dict(self):
        return {
            'start_node_properties': self.start_node_properties,
            'end_node_properties': self.end_node_properties,
            'properties': self.properties
        }
