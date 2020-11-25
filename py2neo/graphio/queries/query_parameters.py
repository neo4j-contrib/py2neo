def params_create_rels_unwind_from_objects(relationships, property_identifier=None):
    """
    Format Relationship properties into a one level dictionary matching the query generated in
    `query_create_rels_from_list`. This is necessary because you cannot access nested dictionairies
    in the UNWIND query.

    UNWIND { rels } AS rel
    MATCH (a:Gene), (b:GeneSymbol)
    WHERE a.sid = rel.start_sid AND b.sid = rel.end_sid AND b.taxid = rel.end_taxid
    CREATE (a)-[r:MAPS]->(b)
    SET r = rel.properties

    Call with params:
        {'start_sid': 1, 'end_sid': 2, 'end_taxid': '9606', 'properties': {'foo': 'bar} }

    :param relationships: List of Relationships.
    :return: List of parameter dictionaries.
    """
    if not property_identifier:
        property_identifier = 'rels'

    output = []

    for r in relationships:
        d = {}
        for k, v in r.start_node_properties.items():
            d['start_{}'.format(k)] = v
        for k, v in r.end_node_properties.items():
            d['end_{}'.format(k)] = v
        d['properties'] = r.properties
        output.append(d)

    return {property_identifier: output}
