# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Tests for the Git LFS storage plugin."""

import hashlib
import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from snakemake_storage_plugin_cached_http import (
    FileMetadata,
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
    WrongChecksum,
)
from tests.conftest import assert_no_http_requests

# A real SHA-256 OID and corresponding content for testing
TEST_CONTENT = b'{"test": "data", "value": 42}'
TEST_OID = hashlib.sha256(TEST_CONTENT).hexdigest()
TEST_URL = f"lfs://{TEST_OID}/path/to/test.json"
TEST_DOWNLOAD_URL = "https://lfs-server.example.com/objects/" + TEST_OID


@pytest.fixture
def test_logger():
    return logging.getLogger("test")


@pytest.fixture
def storage_provider(tmp_path, test_logger):
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        repo_url="https://github.com/org/repo",
        token_envvar="",
        local_repo="",
        cache="",
        skip_remote_checks=False,
        max_concurrent_downloads=3,
    )

    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )

    return provider


@pytest.fixture
def storage_provider_with_cache(tmp_path, test_logger):
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir()
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        repo_url="https://github.com/org/repo",
        token_envvar="",
        local_repo="",
        cache=str(cache_dir),
        skip_remote_checks=False,
        max_concurrent_downloads=3,
    )

    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )

    return provider


@pytest.fixture
def storage_object(storage_provider):
    return StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )


def make_lfs_batch_response(oid: str, download_url: str, size: int) -> dict:
    """Create a mock LFS batch API response."""
    return {
        "transfer": "basic",
        "objects": [
            {
                "oid": oid,
                "size": size,
                "actions": {
                    "download": {
                        "href": download_url,
                        "header": {},
                    }
                },
            }
        ],
    }


def make_lfs_batch_not_found_response(oid: str) -> dict:
    """Create a mock LFS batch API response for a missing object."""
    return {
        "transfer": "basic",
        "objects": [
            {
                "oid": oid,
                "size": 0,
                "error": {"code": 404, "message": "Object not found"},
            }
        ],
    }


@pytest.fixture
def mock_lfs_server(monkeypatch):
    """
    Fixture that mocks the LFS batch API and download endpoint.

    Returns a dict with the mock objects so tests can inspect calls.
    """
    batch_response_data = make_lfs_batch_response(TEST_OID, TEST_DOWNLOAD_URL, len(TEST_CONTENT))

    async def mock_post(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = batch_response_data
        return resp

    @asynccontextmanager
    async def mock_stream(method, url, **kwargs):
        resp = AsyncMock()
        resp.status_code = 200
        resp.headers = {"content-length": str(len(TEST_CONTENT))}

        async def aiter_bytes(chunk_size=8192):
            yield TEST_CONTENT

        resp.aiter_bytes = aiter_bytes
        yield resp

    return {
        "batch_response": batch_response_data,
        "mock_post": mock_post,
        "mock_stream": mock_stream,
    }


def test_storage_object_parsing():
    """Test that lfs:// URLs are parsed correctly."""
    settings = StorageProviderSettings(repo_url="https://github.com/org/repo")
    provider = StorageProvider(
        local_prefix=MagicMock(),
        logger=logging.getLogger("test"),
        settings=settings,
    )
    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    assert obj.oid == TEST_OID
    assert obj.lfs_path == "path/to/test.json"


def test_lfs_batch_api_url():
    """Test that the LFS batch API URL is constructed correctly."""
    settings = StorageProviderSettings(repo_url="https://github.com/org/repo")
    provider = StorageProvider(
        local_prefix=MagicMock(),
        logger=logging.getLogger("test"),
        settings=settings,
    )

    url = provider._lfs_batch_api_url()
    assert url == "https://github.com/org/repo.git/info/lfs/objects/batch"


def test_lfs_batch_api_url_trailing_slash():
    """Test that trailing slashes in repo_url are handled."""
    settings = StorageProviderSettings(repo_url="https://github.com/org/repo/")
    provider = StorageProvider(
        local_prefix=MagicMock(),
        logger=logging.getLogger("test"),
        settings=settings,
    )

    url = provider._lfs_batch_api_url()
    assert url == "https://github.com/org/repo.git/info/lfs/objects/batch"


@pytest.mark.asyncio
async def test_get_metadata_from_batch_api(storage_provider, mock_lfs_server):
    """Test that get_metadata correctly queries the LFS batch API."""
    mock_client = MagicMock()
    mock_client.post = mock_lfs_server["mock_post"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_provider.client = mock_client_ctx

    metadata = await storage_provider.get_metadata(TEST_OID)

    assert metadata is not None
    assert metadata.size == len(TEST_CONTENT)
    assert metadata.checksum == f"sha256:{TEST_OID}"
    assert metadata.download_url == TEST_DOWNLOAD_URL


@pytest.mark.asyncio
async def test_get_metadata_not_found(storage_provider):
    """Test that get_metadata returns None for missing objects."""
    not_found_oid = "a" * 64

    async def mock_post(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = make_lfs_batch_not_found_response(not_found_oid)
        return resp

    mock_client = MagicMock()
    mock_client.post = mock_post

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_provider.client = mock_client_ctx

    metadata = await storage_provider.get_metadata(not_found_oid)
    assert metadata is None


@pytest.mark.asyncio
async def test_managed_exists_with_metadata(storage_object, mock_lfs_server):
    """Test managed_exists returns True when LFS object exists."""
    mock_client = MagicMock()
    mock_client.post = mock_lfs_server["mock_post"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_object.provider.client = mock_client_ctx

    exists = await storage_object.managed_exists()
    assert exists is True


@pytest.mark.asyncio
async def test_managed_mtime_is_zero(storage_object):
    """LFS objects are immutable so mtime is always 0."""
    mtime = await storage_object.managed_mtime()
    assert mtime == 0


@pytest.mark.asyncio
async def test_managed_size(storage_object, mock_lfs_server):
    """Test that managed_size returns the size from LFS batch API."""
    mock_client = MagicMock()
    mock_client.post = mock_lfs_server["mock_post"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_object.provider.client = mock_client_ctx

    size = await storage_object.managed_size()
    assert size == len(TEST_CONTENT)


@pytest.mark.asyncio
async def test_skip_remote_checks(tmp_path, test_logger):
    """Test that skip_remote_checks returns defaults without API calls."""
    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        repo_url="https://github.com/org/repo",
        skip_remote_checks=True,
    )
    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )
    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    assert await obj.managed_exists() is True
    assert await obj.managed_mtime() == 0
    assert await obj.managed_size() == 0


@pytest.mark.asyncio
async def test_download_and_checksum(storage_object, mock_lfs_server, tmp_path):
    """Test downloading an LFS object and verifying its checksum."""
    local_path = tmp_path / "test_download" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    storage_object.local_path = lambda: local_path

    # Pre-populate metadata cache so we don't need to mock POST
    storage_object.provider._lfs_metadata_cache[TEST_OID] = FileMetadata(
        checksum=f"sha256:{TEST_OID}",
        size=len(TEST_CONTENT),
        download_url=TEST_DOWNLOAD_URL,
        download_headers={},
    )

    mock_client = MagicMock()
    mock_client.stream = mock_lfs_server["mock_stream"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_object.provider.client = mock_client_ctx

    await storage_object.managed_retrieve()

    assert local_path.exists()
    assert local_path.read_bytes() == TEST_CONTENT

    # Verify checksum passes
    storage_object.verify_checksum(local_path)


def test_wrong_checksum_detection(storage_object, tmp_path):
    """Test that corrupted files are detected via checksum."""
    corrupted_path = tmp_path / "corrupted.json"
    corrupted_path.write_bytes(b'{"corrupted": "data"}')

    with pytest.raises(WrongChecksum):
        storage_object.verify_checksum(corrupted_path)


@pytest.mark.asyncio
async def test_cache_stores_and_retrieves(storage_provider_with_cache, mock_lfs_server, tmp_path):
    """Test that files are cached after download and reused on second access."""
    obj1 = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider_with_cache,
    )

    local_path1 = tmp_path / "download1" / "test.json"
    local_path1.parent.mkdir(parents=True, exist_ok=True)
    obj1.local_path = lambda: local_path1

    # Inject metadata
    storage_provider_with_cache._lfs_metadata_cache[TEST_OID] = FileMetadata(
        checksum=f"sha256:{TEST_OID}",
        size=len(TEST_CONTENT),
        download_url=TEST_DOWNLOAD_URL,
        download_headers={},
    )

    mock_client = MagicMock()
    mock_client.stream = mock_lfs_server["mock_stream"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_provider_with_cache.client = mock_client_ctx

    await obj1.managed_retrieve()

    # Verify cache was populated
    assert storage_provider_with_cache.cache is not None
    cached_path = storage_provider_with_cache.cache.get(TEST_URL)
    assert cached_path is not None
    assert cached_path.exists()

    # Second download should use cache (no HTTP requests)
    obj2 = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider_with_cache,
    )
    local_path2 = tmp_path / "download2" / "test.json"
    local_path2.parent.mkdir(parents=True, exist_ok=True)
    obj2.local_path = lambda: local_path2

    with assert_no_http_requests(storage_provider_with_cache):
        await obj2.managed_retrieve()

    assert local_path1.read_bytes() == local_path2.read_bytes()


@pytest.mark.asyncio
async def test_local_repo_lookup(tmp_path, test_logger):
    """Test that files are found in the local git LFS store."""
    # Create a fake local repo with the LFS object
    repo_dir = tmp_path / "repo"
    lfs_obj_dir = repo_dir / ".git" / "lfs" / "objects" / TEST_OID[:2] / TEST_OID[2:4]
    lfs_obj_dir.mkdir(parents=True)
    lfs_obj_path = lfs_obj_dir / TEST_OID
    lfs_obj_path.write_bytes(TEST_CONTENT)

    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        repo_url="https://github.com/org/repo",
        local_repo=str(repo_dir),
        cache="",
    )
    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )

    # Verify _find_local_lfs_object finds it
    found = provider._find_local_lfs_object(TEST_OID)
    assert found is not None
    assert found == lfs_obj_path

    # Verify managed_retrieve uses the local object (no HTTP)
    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    local_path = tmp_path / "out" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    with assert_no_http_requests(provider):
        await obj.managed_retrieve()

    assert local_path.read_bytes() == TEST_CONTENT


@pytest.mark.asyncio
async def test_local_repo_oid_mismatch_falls_through(tmp_path, test_logger, mock_lfs_server):
    """Test that a local LFS object with wrong OID triggers a warning and falls through to download."""
    wrong_content = b"this is wrong content"
    expected_oid = TEST_OID  # we still expect to download TEST_CONTENT

    # Create a fake local repo with wrong content stored under the right OID path
    repo_dir = tmp_path / "repo"
    lfs_obj_dir = repo_dir / ".git" / "lfs" / "objects" / TEST_OID[:2] / TEST_OID[2:4]
    lfs_obj_dir.mkdir(parents=True)
    lfs_obj_path = lfs_obj_dir / TEST_OID
    lfs_obj_path.write_bytes(wrong_content)  # wrong content -> OID mismatch

    local_prefix = tmp_path / "local"
    local_prefix.mkdir()

    settings = StorageProviderSettings(
        repo_url="https://github.com/org/repo",
        local_repo=str(repo_dir),
        cache="",
    )
    provider = StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )

    # Inject metadata
    provider._lfs_metadata_cache[TEST_OID] = FileMetadata(
        checksum=f"sha256:{TEST_OID}",
        size=len(TEST_CONTENT),
        download_url=TEST_DOWNLOAD_URL,
        download_headers={},
    )

    mock_client = MagicMock()
    mock_client.stream = mock_lfs_server["mock_stream"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    provider.client = mock_client_ctx

    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    local_path = tmp_path / "out" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    with pytest.warns(UserWarning, match="OID mismatch"):
        await obj.managed_retrieve()

    # Should have downloaded the correct content
    assert local_path.read_bytes() == TEST_CONTENT
