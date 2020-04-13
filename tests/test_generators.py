import os

import pytest
from dateutil.parser import parse

from feldspar.generators import XESImporter, TraceGenerator, _PickleImporter

from . import (RUNNING_EXAMPLE_XES_PATH, RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH,
               RUNNING_EXAMPLE_XES_GZ_PATH, RUNNING_EXAMPLE_XES_ZIP_PATH)
from .fixtures import tmp_dat_file


class TestXESImporter:

    def test_construct_default_object(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        assert isinstance(it, XESImporter)

    def test_construct_gz_compressed_object(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_GZ_PATH, "gz")
        assert isinstance(it, XESImporter)

    def test_construct_zip_compressed_object(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_ZIP_PATH, "zip")
        assert isinstance(it, XESImporter)

    def test_construct_gz_compressed_object_by_infering_compression(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_GZ_PATH, "infer")
        assert isinstance(it, XESImporter)

    def test_construct_zip_compressed_object_by_infering_compression(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_ZIP_PATH, "infer")
        assert isinstance(it, XESImporter)

    def test_construct_compressed_object_wo_parameter_or_compression_inference(self):
        with pytest.warns(Warning):
            XESImporter(RUNNING_EXAMPLE_XES_ZIP_PATH)

    def test_produce_iterator(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        assert isinstance(iter(it), XESImporter)

    def test_iterate_over_object(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        assert len(list(it)) == 6

    def test_extract_meta_attributes(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        attributes = it._extract_meta()
        target = {
            "creator": "Fluxicon Nitro"
        }
        assert attributes['attributes'] == target

    def test_extract_meta_classifiers(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        attributes = it._extract_meta()
        target = {
            "Activity": {"name": "Activity", "keys": "Activity"},
            "activity classifier": {"name": "activity classifier", "keys": "Activity"}
        }
        assert attributes['classifiers'] == target

    def test_extract_meta_extensions(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        attributes = it._extract_meta()
        target = {
            "Concept": {"name": "Concept", "prefix": "concept", "uri": "http://code.deckfour.org/xes/concept.xesext"},
            "Time": {"name": "Time", "prefix": "time", "uri": "http://code.deckfour.org/xes/time.xesext"},
            "Organizational": {"name": "Organizational", "prefix": "org", "uri": "http://code.deckfour.org/xes/org.xesext"}
        }
        assert attributes['extensions'] == target

    def test_extract_meta_omni(self):
        it = XESImporter(RUNNING_EXAMPLE_XES_PATH)
        attributes = it._extract_meta()
        target = {
            "trace": {"concept:name": "name"},
            "event": {
                "concept:name": "name",
                "org:resource": "resource",
                "time:timestamp": parse("2011-04-13T14:02:31.199+02:00"),
                "Activity": "string",
                "Resource": "string",
                "Costs": "string",
            }
        }
        assert attributes['omni'] == target


class TestTraceGenerator:

    def test_from_file_iterator(self):
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        assert len(list(L)) == 6

    def test_from_file_iterator_multiple_pass_throughs(self):
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        assert len(list(L)) == 6
        assert len(list(L)) == 6


class TestElementGeneratorCaching:
    def test_cache_swapping_sources(self):
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache()
        # Before first iteration previous generator should be used
        assert isinstance(L._source, TraceGenerator)
        assert len(list(L)) == 6
        # After iteration, everything should be cached
        assert isinstance(L._source, list)
        assert len(list(L)) == 6

    def test_cache_correct_element_reproduction(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache()
        list(L)
        assert all(x == y for x, y in zip(L, target))

    def test_cache_to_file_swapping_sources(self, tmp_dat_file):
        assert os.path.getsize(tmp_dat_file) == 0
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache(tmp_dat_file)
        # Even in the first iteration element aren cached in file
        assert os.path.getsize(tmp_dat_file) != 0
        assert isinstance(L._source, _PickleImporter)
        assert len(list(L)) == 6

    def test_cache_to_file_correct_element_reproduction(self, tmp_dat_file):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache(tmp_dat_file)
        assert all(x == y for x, y in zip(L, target))

    def test_cache_before_caching_correct_attributes(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache()
        assert L.attributes == target.attributes

    def test_cache_after_caching_correct_attributes(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache()
        list(L)
        assert L.attributes == target.attributes

    def test_cache_to_file_after_caching_correct_attributes(self, tmp_dat_file):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.cache(tmp_dat_file)
        assert L.attributes == target.attributes

    def test_cache_multiple_iterations(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        assert L.attributes == target.attributes
        L = L.cache()
        assert L.attributes == target.attributes
        assert all(x == y for x, y in zip(L, target))
        assert all(x == y for x, y in zip(L, target))
        assert all(x == y for x, y in zip(L, target))
        assert L.attributes == target.attributes

    def test_cache_to_file_multiple_iterations(self, tmp_dat_file):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        assert L.attributes == target.attributes
        L = L.cache(tmp_dat_file)
        assert L.attributes == target.attributes
        assert all(x == y for x, y in zip(L, target))
        assert all(x == y for x, y in zip(L, target))
        assert all(x == y for x, y in zip(L, target))
        assert L.attributes == target.attributes

    def test_cache_to_file_persistent_file(self, tmp_dat_file):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L0 = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L0 = L0.cache(tmp_dat_file)

        L1 = TraceGenerator.from_file(RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH)
        L1 = L1.cache(tmp_dat_file)

        assert all(x == y for x, y in zip(L1, target))

    def test_filter_then_cache(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.filter(lambda trace: len(trace) <= 5)
        L = L.cache()

        assert all(x == y for x, y in zip(L, target))


class TestElementGeneratorFiltering:
    def test_filter(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.filter(lambda trace: len(trace) <= 5)
        assert all(x == y for x, y in zip(L, target))

    def test_filter_multiple_pass_throughs(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.filter(lambda trace: len(trace) <= 5)
        assert all(x == y for x, y in zip(L, target))
        assert all(x == y for x, y in zip(L, target))

    def test_filter_chaining(self):
        target = TraceGenerator.from_file(RUNNING_EXAMPLE_TRACES_LEN_LTEQ_5_PATH)
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        L = L.filter(lambda trace: len(trace) <= 5)
        L = L.filter(lambda trace: trace[-1]["concept:name"] == "pay compensation" )
        assert len(list(L)) == 2  

