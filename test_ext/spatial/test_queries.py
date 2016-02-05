from unittest import skipUnless
from py2neo.ext.spatial.plugin import NAME_PROPERTY
from py2neo.ext.spatial.exceptions import LayerNotFoundError
from .basetest import spatial_available, SpatialTestCase


class QueryTest(SpatialTestCase):

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_find_from_bad_layer(self, spatial, uk_features):

        likeable_stoke_newington_character = (51.559676, -0.07937)

        with self.assertRaises(LayerNotFoundError):
            spatial.find_within_distance(
                "america", likeable_stoke_newington_character, 1000)

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_find_everything(self):

        self.load(self.spatial, self.uk_features, "uk")

        likeable_stoke_newington_character = (51.559676, -0.07937)

        pois = self.spatial.find_within_distance(
            "uk", likeable_stoke_newington_character, 1000)

        assert len(pois) == len(self.uk_features)

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_find_only_within_london(self):
        self.load(self.spatial, self.uk_features, "uk")
        self.load(self.spatial, self.london_features, "uk")

        # london_features *are* on the uk_layer - see conftest.py
        assert len(self.uk_features) > len(self.london_features)

        likeable_stoke_newington_character = (51.559676, -0.07937)

        # this should only return london features
        pois = self.spatial.find_within_distance(
            "uk", likeable_stoke_newington_character, 35)

        assert len(pois) == len(self.london_features)

        # check it really does
        expected_london_names = sorted([f.name for f in self.london_features])
        returned_names = sorted(
            [poi.get_properties()[NAME_PROPERTY] for poi in pois])
        assert expected_london_names == returned_names

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_find_closest_geometries_single_layer(self):

        self.load(self.spatial, self.cornish_towns, "cornwall")
        tourist = (50.500000, -4.500000)  # Bodmin, Cornwall
        pois = self.spatial.find_closest_geometries(tourist)

        assert len(pois) == len(self.cornish_towns)

        expected_towns = sorted([f.name for f in self.cornish_towns])
        returned_towns = sorted(
            [poi.get_properties()[NAME_PROPERTY] for poi in pois])
        assert expected_towns == returned_towns

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_find_closest_geometries_over_more_layers(self):

        self.load(self.spatial, self.cornish_towns, "cornwall")
        self.load(self.spatial, self.devonshire_towns, "devon")

        tourist = (50.500000, -4.500000)  # Bodmin, Cornwall
        pois = self.spatial.find_closest_geometries(tourist)

        assert len(pois) == len(self.cornish_towns) + len(self.devonshire_towns)

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_london_not_close(self):
        self.load(self.spatial, self.cornish_towns, "cornwall")
        self.load(self.spatial, self.london_features, "london")
        tourist = (50.500000, -4.500000)  # Bodmin, Cornwall
        pois = self.spatial.find_closest_geometries(tourist)

        assert len(pois) == len(self.cornish_towns)

        expected_towns = sorted([f.name for f in self.cornish_towns])
        returned_towns = sorted(
            [poi.get_properties()[NAME_PROPERTY] for poi in pois])
        assert expected_towns == returned_towns

    @skipUnless(spatial_available, "no spatial plugin available")
    def test_find_within_bounding_box(self):

        self.load(self.spatial, self.london_features, "uk")
        london = (51.236220, -0.570409, 51.703000, 0.244)
        min_longitude, min_latitude, max_longitude, max_latitude = london
        pois = self.spatial.find_within_bounding_box(
            "uk", min_longitude, min_latitude, max_longitude, max_latitude)

        assert len(pois) == len(self.london_features)

        expected_features = sorted([f.name for f in self.london_features])
        actual_features = sorted(
            [poi.get_properties()[NAME_PROPERTY] for poi in pois])
        assert expected_features == actual_features
