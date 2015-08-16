class TestBase(object):
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
    def _geometry_exists(graph, geometry_name):
        response = graph.cypher.execute(
            "match (n)<-[:RTREE_REFERENCE]-() "
            "where n.geometry_name = '{}' return n".format(geometry_name)
        )
        results = list(response)
        return len(results) == 1

    @staticmethod
    def _node_exists(graph, node_id):
        response = graph.cypher.execute(
            "match (n) where id(n) = {} return n".format(node_id))
        results = list(response)
        return len(results) == 1

    @staticmethod
    def load_geometries(api, geometries, layer):
        api.create_layer(layer)
        for geometry in geometries:
            geometry_name, wkt_string = geometry
            api.create_geometry(
                geometry_name=geometry_name, wkt_string=wkt_string,
                layer_name=layer)

    @staticmethod
    def load_points_of_interest(api, data, layer_name, labels=None):
        labels = labels or []
        for location in data:
            lat, lon = location.coords
            api.create_point_of_interest(
                location.name, layer_name, lon, lat, labels)

    @staticmethod
    def get_geometry_node(api, geometry_name):
        query = """MATCH (geometry_node {geometry_name:{geometry_name}})
RETURN geometry_node"""
        params = {
            'geometry_name': geometry_name,
        }

        result = api.graph.cypher.execute(query, params)
        record = result[0]
        geometry_node = record[0]
        return geometry_node
