import pytest

from . import TestBase
from py2neo.ext.spatial.plugin import NAME_PROPERTY
from py2neo.ext.spatial.exceptions import LayerNotFoundError, ValidationError


class TestQueries(TestBase):
    def test_try_and_find_from_bad_layer(self, spatial, uk_features):
        stoke_newington = (51.559676, -0.07937)

        with pytest.raises(LayerNotFoundError):
            spatial.find_within_distance(
                layer_name="america",  # does not exist
                longitude=stoke_newington[1],
                latitude=stoke_newington[0],
                distance=1000,
            )

    def test_find_all_points_of_interest(
            self, spatial, uk, towns, uk_features, london_features):

        expected_total = sum(
            [len(uk_features), len(towns), len(london_features)]
        )

        self.load_points_of_interest(spatial, towns, "uk")
        self.load_points_of_interest(spatial, uk_features, "uk")
        self.load_points_of_interest(spatial, london_features, "uk")

        stoke_newington = (51.559676, -0.07937)

        pois = spatial.find_within_distance(
            layer_name="uk",
            longitude=stoke_newington[1],
            latitude=stoke_newington[0],
            distance=800,
        )

        assert len(pois) == expected_total

    def test_find_only_within_london(
            self, spatial, uk, uk_features, london_features):

        self.load_points_of_interest(spatial, uk_features, "uk")
        self.load_points_of_interest(spatial, london_features, "uk")

        # london_features *are* on the uk_layer - see conftest.py
        assert len(uk_features) > len(london_features)

        stoke_newington = (51.559676, -0.07937)

        # this should only return london features
        pois = spatial.find_within_distance(
            layer_name="uk",
            latitude=stoke_newington[0],
            longitude=stoke_newington[1],
            distance=35,
        )

        assert len(pois) == len(london_features)

        # check it really does
        expected_london_names = sorted([f.name for f in london_features])
        returned_names = sorted(
            [poi.get_properties()[NAME_PROPERTY] for poi in pois])
        assert expected_london_names == returned_names

    def test_find_within_bounding_box_of_london(
            self, spatial, uk, london_features, uk_features, towns):

        assert len(london_features) == 4

        self.load_points_of_interest(spatial, london_features, "uk")
        self.load_points_of_interest(spatial, uk_features, "uk")
        self.load_points_of_interest(spatial, towns, "uk")

        london = (51.236220, -0.570409, 51.703000, 0.244)
        min_latitude, min_longitude, max_latitude, max_longitude = london
        pois = spatial.find_within_bounding_box(
            "uk", min_longitude, min_latitude, max_longitude, max_latitude)

        assert len(pois) == len(london_features)

        expected_features = sorted([f.name for f in london_features])
        actual_features = sorted(
            [poi.get_properties()[NAME_PROPERTY] for poi in pois])
        assert expected_features == actual_features

    def test_finds_multi_geometry_types_within_distance(
            self, spatial, counties, london_features):

        self.load_points_of_interest(spatial, london_features, "uk")

        # somewhere in North London
        longitude = -0.07937
        latitude = 51.559676

        # this should only return london features
        pois = spatial.find_within_distance(
            layer_name="uk",
            longitude=longitude,
            latitude=latitude,
            distance=35,
        )

        points_of_interest = [
            node for node in pois if 'Point' in node.labels]

        counties = [
            node for node in pois if 'MultiPolygon' in node. labels]

        assert len(points_of_interest) + len(counties) == len(pois)
        assert len(points_of_interest) == len(london_features)

        expected_counties = [
            'london', 'surrey', 'buckinghamshire', 'essex', 'kent'
        ]

        county_names = [
            county_node.properties['geometry_name']
            for county_node in counties
        ]

        assert sorted(expected_counties) == sorted(county_names)

    @pytest.mark.parametrize("latitude, longitude, expected_location", [
        (51.127889, -3.003632, "somerset"),  # Bridgewater
        (51.059771, -1.310142, "hampshire"),  # Winchester
        (53.763201, -2.703090, "lancashire"),  # Preston
        (51.236220, -0.570409, "surrey"),  # Guildford
        (51.068785, -1.794472, "wiltshire"),  # Salisbury
        (51.752021, -1.257726, "oxfordshire"),  # Oxford
    ])
    def test_find_which_county_contains_poi(
            self, spatial, counties, latitude, longitude, expected_location):

        geometries = spatial.find_containing_geometries(
            layer_name="uk", longitude=longitude, latitude=latitude)

        assert len(geometries) == 1

        geometry = geometries[0]
        assert geometry['geometry_name'] == expected_location

    # TODO: add england?
    @pytest.mark.parametrize("latitude, longitude, expected_location", [
        (51.127889, -3.003632, "somerset"),  # Bridgewater
        (51.059771, -1.310142, "hampshire"),  # Winchester
        (53.763201, -2.703090, "lancashire"),  # Preston
        (51.236220, -0.570409, "surrey"),  # Guildford
        (51.068785, -1.794472, "wiltshire"),  # Salisbury
        (51.752021, -1.257726, "oxfordshire"),  # Oxford
    ])
    def test_find_all_geometries_that_contains_poi(
            self, spatial, counties, latitude, longitude, expected_location,
            england):

        geometries = spatial.find_containing_geometries(
            layer_name="uk", longitude=longitude, latitude=latitude)

        assert len(geometries) == 2  # includes england now

    def test_find_points_of_interest_missing_layer(self, spatial):
        # somewhere in North London
        longitude = -0.07937
        latitude = 51.559676

        with pytest.raises(LayerNotFoundError):
            spatial.find_points_of_interest(
                layer_name="does_not_exist",
                longitude=longitude,
                latitude=latitude,
                max_distance=50,
                labels=[]
            )

    def test_find_points_of_interest_without_labels(self, spatial, uk):
        # somewhere in North London
        longitude = -0.07937
        latitude = 51.559676

        with pytest.raises(ValidationError):
            spatial.find_points_of_interest(
                layer_name="uk", longitude=longitude, latitude=latitude,
                max_distance=50, labels=[])

    def test_find_points_of_interest(self, spatial, uk, london_features):
        assert len(london_features) == 4

        # somewhere in North London
        longitude = -0.07937
        latitude = 51.559676
        max_distance = 50

        favourite_london_feature = london_features.pop()

        self.load_points_of_interest(
            spatial, [favourite_london_feature], "uk",
            labels=['london', 'favourite'])
        self.load_points_of_interest(
            spatial, london_features, "uk", labels=['london'])

        geometries = spatial.find_points_of_interest(
            layer_name="uk", longitude=longitude, latitude=latitude,
            max_distance=max_distance, labels=['london'])

        assert len(geometries) == 4

        geometries = spatial.find_points_of_interest(
            layer_name="uk", longitude=longitude, latitude=latitude,
            max_distance=max_distance, labels=['favourite'])

        assert len(geometries) == 1
