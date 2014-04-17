# coding=utf-8
import pytest
from py2neo.exceptions import ClientError

__author__ = 'meier_ul'

from py2neo import neo4j, node


def test_schema_index():
    graph_db = neo4j.GraphDatabaseService()
    if not graph_db.supports_schema_indexes:
        return

    graph_db.clear()
    for label in graph_db.node_labels:
        for key in graph_db.schema.get_uniqueness_constraints(label):
            graph_db.schema.drop_uniqueness_constraint(label, key)
        for key in graph_db.schema.get_indexed_property_keys(label):
            graph_db.schema.drop_index(label, key)

    muenchen, = graph_db.create(node(name=u"München", schluessel="09162000"))
    muenchen.add_labels("Gemeinde", "Kreis")
    graph_db.schema.create_index("Gemeinde", "name")
    graph_db.schema.create_index("Gemeinde", "schluessel")
    graph_db.schema.create_index("Kreis", "name")
    graph_db.schema.create_index("Kreis", "schluessel")
    found_gemeinde_name = graph_db.find("Gemeinde", "name", u"München")
    found_gemeinde_schluessel = graph_db.find("Gemeinde", "schluessel", "09162000")
    found_kreis_name = graph_db.find("Kreis", "name", u"München")
    found_kreis_schluessel = graph_db.find("Kreis", "schluessel", "09162000")
    assert list(found_gemeinde_name) == list(found_gemeinde_schluessel)
    assert list(found_kreis_name) == list(found_kreis_schluessel)
    assert list(found_gemeinde_name) == list(found_kreis_name)
    keys = graph_db.schema.get_indexed_property_keys("Gemeinde")
    assert "name" in keys
    assert "schluessel" in keys
    graph_db.schema.drop_index("Gemeinde", "name")
    graph_db.schema.drop_index("Gemeinde", "schluessel")
    graph_db.schema.drop_index("Kreis", "name")
    graph_db.schema.drop_index("Kreis", "schluessel")
    with pytest.raises(LookupError):
        graph_db.schema.drop_index("Kreis", "schluessel")

    taufkirchen, = graph_db.create(node(name=u"Taufkirchen"))
    taufkirchen.add_labels("Gemeinde")
    graph_db.schema.create_uniqueness_constraint("Gemeinde", "name")
    constraints = graph_db.schema.get_uniqueness_constraints("Gemeinde")
    assert "name" in constraints
    taufkirchen2, = graph_db.create(node(name=u"Taufkirchen"))
    with pytest.raises(ValueError):
        taufkirchen2.add_labels("Gemeinde")






