from .. util import parse_lat_long


class Base(object):
    @staticmethod
    def _layer_exists(graph, layer_name):
        results = graph.cypher.execute(
            "MATCH (r { name:'spatial_root' }), (r)-[:LAYER]->(n) RETURN n")

        for record in results:
            node = record[0]
            if node.properties['layer'] == layer_name:
                return True
        return False

    @staticmethod
    def _geometry_exists(graph, geometry_name, LAYER_NAME):
        # assert a geometry exists in the *application* graph
        resp = graph.find(
            label=LAYER_NAME, property_key="name",
            property_value=geometry_name)
        results = [r for r in resp]

        return len(results) == 1

    @staticmethod
    def load(api, data, layer):
        api.create_layer(layer)
        for location in data:
            shape = parse_lat_long(location.coords)
            api.create_geometry(location.name, shape.wkt, layer)
