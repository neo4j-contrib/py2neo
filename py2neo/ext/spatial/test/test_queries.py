import pytest

from . import Base
from ..exceptions import LayerNotFoundError


class TestQueries(Base):
    def test_find_from_bad_layer(
            self, spatial, uk_features):

        likeable_stoke_newington_character = (51.559676, -0.07937)

        with pytest.raises(LayerNotFoundError):
            spatial.find_within_distance(
                "america", likeable_stoke_newington_character, 1000)

    def test_find_everything(
            self, spatial, uk_features):

        self.load(spatial, uk_features, "uk")

        likeable_stoke_newington_character = (51.559676, -0.07937)

        pois = spatial.find_within_distance(
            "uk", likeable_stoke_newington_character, 1000)

        assert len(pois) == len(uk_features)

    def test_find_only_within_london(
            self, spatial, uk_features, london_features):

        self.load(spatial, uk_features, "uk")
        self.load(spatial, london_features, "uk")

        # london_features *are* on the uk_layer - see conftest.py
        assert len(uk_features) > len(london_features)

        likeable_stoke_newington_character = (51.559676, -0.07937)

        # this should only return london features
        pois = spatial.find_within_distance(
            "uk", likeable_stoke_newington_character, 35)

        assert len(pois) == len(london_features)

        # check it really does
        expected_london_names = sorted([f.name for f in london_features])
        returned_names = sorted([poi.get_properties()['name'] for poi in pois])
        assert expected_london_names == returned_names

    def test_find_closest_geometries_single_layer(
            self, spatial, cornish_towns):

        self.load(spatial, cornish_towns, "cornwall")
        tourist = (50.500000, -4.500000)  # Bodmin, Cornwall
        pois = spatial.find_closest_geometries(tourist)

        assert len(pois) == len(cornish_towns)

        expected_towns = sorted([f.name for f in cornish_towns])
        returned_towns = sorted([poi.get_properties()['name'] for poi in pois])
        assert expected_towns == returned_towns

    def test_find_closest_geometries_over_more_layers(
            self, spatial, cornish_towns, devonshire_towns):

        self.load(spatial, cornish_towns, "cornwall")
        self.load(spatial, devonshire_towns, "devon")

        tourist = (50.500000, -4.500000)  # Bodmin, Cornwall
        pois = spatial.find_closest_geometries(tourist)

        assert len(pois) == len(cornish_towns) + len(devonshire_towns)

    def test_london_not_close(self, spatial, cornish_towns, london_features):
        self.load(spatial, cornish_towns, "cornwall")
        self.load(spatial, london_features, "london")
        tourist = (50.500000, -4.500000)
        pois = spatial.find_closest_geometries(tourist)

        assert len(pois) == len(cornish_towns)

        expected_towns = sorted([f.name for f in cornish_towns])
        returned_towns = sorted([poi.get_properties()['name'] for poi in pois])
        assert expected_towns == returned_towns
