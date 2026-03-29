"""Tests for GraphManaClient interface and helpers."""

import inspect

import numpy as np
import pandas as pd

from graphmana_py import GraphManaClient, __version__
from graphmana_py._unpack import unpack_genotypes


class TestClientInterface:
    """Verify GraphManaClient class interface."""

    def test_class_exists(self):
        assert GraphManaClient is not None

    def test_version(self):
        assert __version__ == "0.1.0"

    def test_has_connect_method(self):
        assert hasattr(GraphManaClient, "connect")
        assert callable(GraphManaClient.connect)

    def test_has_close_method(self):
        assert hasattr(GraphManaClient, "close")
        assert callable(GraphManaClient.close)

    def test_has_status_method(self):
        assert hasattr(GraphManaClient, "status")
        assert callable(GraphManaClient.status)

    def test_has_samples_method(self):
        assert hasattr(GraphManaClient, "samples")
        assert callable(GraphManaClient.samples)

    def test_has_populations_method(self):
        assert hasattr(GraphManaClient, "populations")
        assert callable(GraphManaClient.populations)

    def test_has_chromosomes_method(self):
        assert hasattr(GraphManaClient, "chromosomes")
        assert callable(GraphManaClient.chromosomes)

    def test_has_variants_method(self):
        assert hasattr(GraphManaClient, "variants")
        assert callable(GraphManaClient.variants)

    def test_has_genotype_matrix_method(self):
        assert hasattr(GraphManaClient, "genotype_matrix")
        assert callable(GraphManaClient.genotype_matrix)

    def test_has_allele_frequencies_method(self):
        assert hasattr(GraphManaClient, "allele_frequencies")
        assert callable(GraphManaClient.allele_frequencies)

    def test_has_annotation_versions_method(self):
        assert hasattr(GraphManaClient, "annotation_versions")
        assert callable(GraphManaClient.annotation_versions)

    def test_has_cohorts_method(self):
        assert hasattr(GraphManaClient, "cohorts")
        assert callable(GraphManaClient.cohorts)

    def test_has_query_method(self):
        assert hasattr(GraphManaClient, "query")
        assert callable(GraphManaClient.query)

    def test_is_context_manager(self):
        assert hasattr(GraphManaClient, "__enter__")
        assert hasattr(GraphManaClient, "__exit__")


class TestClientSignatures:
    """Verify method signatures have expected parameters."""

    def test_init_defaults(self):
        sig = inspect.signature(GraphManaClient.__init__)
        params = sig.parameters
        assert params["uri"].default == "bolt://localhost:7687"
        assert params["user"].default == "neo4j"
        assert params["password"].default == "graphmana"

    def test_samples_signature(self):
        sig = inspect.signature(GraphManaClient.samples)
        params = list(sig.parameters.keys())
        assert "include_excluded" in params

    def test_variants_signature(self):
        sig = inspect.signature(GraphManaClient.variants)
        params = list(sig.parameters.keys())
        assert "chr" in params
        assert "start" in params
        assert "end" in params

    def test_genotype_matrix_signature(self):
        sig = inspect.signature(GraphManaClient.genotype_matrix)
        params = list(sig.parameters.keys())
        assert "chr" in params
        assert "start" in params
        assert "end" in params

    def test_allele_frequencies_signature(self):
        sig = inspect.signature(GraphManaClient.allele_frequencies)
        params = list(sig.parameters.keys())
        assert "chr" in params
        assert "start" in params
        assert "end" in params

    def test_query_signature(self):
        sig = inspect.signature(GraphManaClient.query)
        params = list(sig.parameters.keys())
        assert "cypher" in params
        assert "params" in params


class TestToDataFrame:
    """Test the _to_df helper method."""

    def test_empty_list(self):
        client = GraphManaClient.__new__(GraphManaClient)
        df = client._to_df([])
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_records_to_df(self):
        client = GraphManaClient.__new__(GraphManaClient)
        records = [
            {"name": "A", "value": 1},
            {"name": "B", "value": 2},
        ]
        df = client._to_df(records)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "value"]
        assert df["name"].tolist() == ["A", "B"]


class TestUnpackGenotypes:
    """Test genotype unpacking (standalone copy)."""

    def test_basic_unpack(self):
        # Pack 4 samples: HomRef(0), Het(1), HomAlt(2), Missing(3)
        # Binary: 11_10_01_00 = 0b11100100 = 0xE4
        packed = bytes([0xE4])
        gt = unpack_genotypes(packed, 4)
        assert gt.dtype == np.int8
        np.testing.assert_array_equal(gt, [0, 1, 2, 3])

    def test_partial_byte(self):
        # 3 samples, last position in byte is padding
        packed = bytes([0xE4])
        gt = unpack_genotypes(packed, 3)
        np.testing.assert_array_equal(gt, [0, 1, 2])

    def test_two_bytes(self):
        # 8 samples across 2 bytes
        # Byte 0: 0,1,2,3 = 0xE4
        # Byte 1: 1,0,3,2 = 0b10_11_00_01 = 0xB1
        packed = bytes([0xE4, 0xB1])
        gt = unpack_genotypes(packed, 8)
        np.testing.assert_array_equal(gt, [0, 1, 2, 3, 1, 0, 3, 2])

    def test_all_homref(self):
        packed = bytes([0x00, 0x00])
        gt = unpack_genotypes(packed, 8)
        np.testing.assert_array_equal(gt, [0, 0, 0, 0, 0, 0, 0, 0])

    def test_all_missing(self):
        packed = bytes([0xFF, 0xFF])
        gt = unpack_genotypes(packed, 8)
        np.testing.assert_array_equal(gt, [3, 3, 3, 3, 3, 3, 3, 3])


class TestClientConstruction:
    """Test client construction without connecting."""

    def test_default_construction(self):
        client = GraphManaClient()
        assert client._uri == "bolt://localhost:7687"
        assert client._user == "neo4j"
        assert client._password == "graphmana"
        assert client._driver is None

    def test_custom_construction(self):
        client = GraphManaClient("bolt://myhost:7688", "admin", "secret")
        assert client._uri == "bolt://myhost:7688"
        assert client._user == "admin"
        assert client._password == "secret"
