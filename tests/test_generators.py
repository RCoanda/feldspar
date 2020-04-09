import os

import pytest
from dateutil.parser import parse

from feldspar.generators import XESImporter, TraceGenerator

from . import RUNNING_EXAMPLE_XES_PATH, RUNNING_EXAMPLE_XES_GZ_PATH, RUNNING_EXAMPLE_XES_ZIP_PATH


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
        attributes = it.extract_meta()
        target = {
            "trace": { "concept:name": "name" },
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

    def test_filter(self):
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        
        L = L.filter(lambda t: int(t["concept:name"]) > 2)
        
        assert len(list(L)) == 4

    def test_filter_multiple_pass_throughs(self):
        L = TraceGenerator.from_file(RUNNING_EXAMPLE_XES_PATH)
        
        L = L.filter(lambda t: int(t["concept:name"]) > 2)
        
        assert len(list(L)) == 4
        assert len(list(L)) == 4