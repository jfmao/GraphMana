"""Tests for QCManager interface and constants."""

import inspect

from graphmana.qc.manager import DEFAULT_BATCH_SIZE, QCManager


class TestQCManagerInterface:
    """Verify QCManager class interface."""

    def test_class_exists(self):
        assert QCManager is not None

    def test_has_run_method(self):
        assert hasattr(QCManager, "run")
        assert callable(QCManager.run)

    def test_has_variant_qc_method(self):
        assert hasattr(QCManager, "variant_qc")
        assert callable(QCManager.variant_qc)

    def test_has_sample_qc_method(self):
        assert hasattr(QCManager, "sample_qc")
        assert callable(QCManager.sample_qc)

    def test_has_batch_qc_method(self):
        assert hasattr(QCManager, "batch_qc")
        assert callable(QCManager.batch_qc)

    def test_run_signature(self):
        sig = inspect.signature(QCManager.run)
        params = list(sig.parameters.keys())
        assert "qc_type" in params

    def test_sample_qc_signature(self):
        sig = inspect.signature(QCManager.sample_qc)
        params = list(sig.parameters.keys())
        assert "batch_size" in params

    def test_default_batch_size(self):
        assert DEFAULT_BATCH_SIZE == 5000
