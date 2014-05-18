#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import unicode_literals
import pytest
from py2neo.exceptions import ServerError
from py2neo import neo4j, node

__author__ = 'Ulrich Meier <ulrich.meier@ldbv.bayern.de'
# Added some methods to handle Constraints. See http://docs.neo4j.org/chunked/stable/rest-api-schema-constraints.html




def get_clean_database():
    graph_db = neo4j.GraphDatabaseService()
    if not graph_db.supports_schema_indexes:
        return None


    # Cleanup the database
    # Constraints have to be removed before the indexed property keys can be removed.
    graph_db.clear()
    for label in graph_db.node_labels:
        for key in graph_db.schema.get_unique_constraints(label):
            graph_db.schema.remove_unique_constraint(label, key)
        for key in graph_db.schema.get_indexed_property_keys(label):
            graph_db.schema.drop_index(label, key)
    return graph_db


def test_schema_index():
    graph_db = get_clean_database()
    if graph_db is None:
        return
    munich, = graph_db.create({'name': "München", 'key': "09162000"})
    munich.add_labels("borough", "county")
    graph_db.schema.create_index("borough", "name")
    graph_db.schema.create_index("borough", "key")
    graph_db.schema.create_index("county", "name")
    graph_db.schema.create_index("county", "key")
    found_borough_via_name = graph_db.find("borough", "name", "München")
    found_borough_via_key = graph_db.find("borough", "key", "09162000")
    found_county_via_name = graph_db.find("county", "name", "München")
    found_county_via_key = graph_db.find("county", "key", "09162000")
    assert list(found_borough_via_name) == list(found_borough_via_key)
    assert list(found_county_via_name) == list(found_county_via_key)
    assert list(found_borough_via_name) == list(found_county_via_name)
    keys = graph_db.schema.get_indexed_property_keys("borough")
    assert "name" in keys
    assert "key" in keys
    graph_db.schema.drop_index("borough", "name")
    graph_db.schema.drop_index("borough", "key")
    graph_db.schema.drop_index("county", "name")
    graph_db.schema.drop_index("county", "key")
    with pytest.raises(LookupError):
        graph_db.schema.drop_index("county", "key")


def test_unique_constraint():
    graph_db = get_clean_database()
    if graph_db is None:
        return
    borough, = graph_db.create(node(name="Taufkirchen"))
    borough.add_labels("borough")
    graph_db.schema.add_unique_constraint("borough", "name")
    constraints = graph_db.schema.get_unique_constraints("borough")
    assert "name" in constraints
    borough_2, = graph_db.create(node(name="Taufkirchen"))
    with pytest.raises(ValueError):
        borough_2.add_labels("borough")


def test_labels_constraints():
    graph_db = get_clean_database()
    if graph_db is None:
        return
    a, b = graph_db.create({"name": "Alice"}, {"name": "Alice"})
    a.add_labels("Person")
    b.add_labels("Person")
    with pytest.raises(ValueError):
        graph_db.schema.add_unique_constraint("Person", "name")
    b.remove_labels('Person')
    graph_db.schema.add_unique_constraint("Person", "name")
    a.remove_labels("Person")
    b.add_labels("Person")
    with pytest.raises(ServerError):
        graph_db.schema.drop_index("Person", "name")
    b.remove_labels("Person")
    graph_db.schema.remove_unique_constraint("Person", "name")
    with pytest.raises(LookupError):
        graph_db.schema.drop_index("Person", "name")







