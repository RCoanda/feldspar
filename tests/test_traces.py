import os

import pytest

from feldspar.traces import XESImporter

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
        assert False
        
    def test_extract_meta_classifiers(self):
        assert False

    def test_extract_meta_extensions(self):
        assert False

    def test_extract_meta_omni(self):
        assert False

