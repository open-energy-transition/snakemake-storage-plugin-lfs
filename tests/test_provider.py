# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Tests for StorageProvider class methods and query validation."""

from snakemake_storage_plugin_lfs import StorageObject, StorageProvider


def test_import_storage_provider():
    assert StorageProvider is not None


def test_import_storage_object():
    assert StorageObject is not None


def test_storage_provider_has_required_methods():
    assert hasattr(StorageProvider, "is_valid_query")
    assert hasattr(StorageProvider, "example_queries")
    assert hasattr(StorageProvider, "get_storage_object_cls")


def test_is_valid_query_lfs():
    result = StorageProvider.is_valid_query("lfs://v1.2.3/path/to/file.csv")
    assert result.valid is True

    result = StorageProvider.is_valid_query("lfs://abc1234/path/to/file.csv")
    assert result.valid is True


def test_is_valid_query_non_lfs():
    result = StorageProvider.is_valid_query("https://example.com/file.txt")
    assert result.valid is False

    result = StorageProvider.is_valid_query(
        "https://zenodo.org/records/123/files/data.csv"
    )
    assert result.valid is False


def test_example_queries():
    examples = StorageProvider.example_queries()
    assert len(examples) > 0
    assert all(hasattr(ex, "query") for ex in examples)
    assert all(hasattr(ex, "description") for ex in examples)
    assert all(ex.query.startswith("lfs://") for ex in examples)
