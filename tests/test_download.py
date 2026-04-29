# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

"""Tests for the Git LFS storage plugin."""

import hashlib
import logging
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest

from snakemake_interface_common.exceptions import WorkflowError
from snakemake_storage_plugin_lfs import (
    DownloadMetadata,
    PointerMetadata,
    StorageObject,
    StorageProvider,
    StorageProviderSettings,
    WrongChecksum,
    utc_after,
)
from tests.conftest import assert_no_http_requests

# A real SHA-256 OID and corresponding content for testing
TEST_CONTENT = b'{"test": "data", "value": 42}'
TEST_OID = hashlib.sha256(TEST_CONTENT).hexdigest()
TEST_REF = "v1.0.0"
TEST_PATH = "path/to/test.json"
TEST_URL = f"lfs://{TEST_REF}/{TEST_PATH}"
TEST_DOWNLOAD_URL = "https://lfs-server.example.com/objects/" + TEST_OID

LFS_POINTER = (
    b"version https://git-lfs.github.com/spec/v1\n"
    + f"oid sha256:{TEST_OID}\n".encode()
    + f"size {len(TEST_CONTENT)}\n".encode()
)


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

    return StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )


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

    return StorageProvider(
        local_prefix=local_prefix,
        logger=test_logger,
        settings=settings,
    )


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


def inject_pointer(provider: StorageProvider, ref: str, path: str, oid: str, size: int):
    """Pre-populate the pointer cache to avoid needing a git API mock."""
    provider.pointer_cache[(ref, path)] = PointerMetadata(oid=oid, size=size)


def inject_download_metadata(provider: StorageProvider, oid: str, download_url: str, size: int):
    """Pre-populate the download metadata cache to avoid needing a batch API mock."""
    provider.download_cache[oid] = DownloadMetadata(
        url=download_url,
        headers={},
        expires_at=utc_after(hours=1),
    )


@pytest.fixture
def mock_lfs_server():
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

    assert obj.ref == TEST_REF
    assert obj.lfs_path == TEST_PATH


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
async def test_get_download_metadata_from_batch_api(storage_provider, mock_lfs_server):
    """Test that get_download_metadata correctly queries the LFS batch API."""
    mock_client = MagicMock()
    mock_client.post = mock_lfs_server["mock_post"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_provider.client = mock_client_ctx

    metadata = await storage_provider.get_download_metadata(TEST_OID)

    assert metadata is not None
    assert metadata.url == TEST_DOWNLOAD_URL
    assert metadata.headers == {}


@pytest.mark.asyncio
async def test_get_download_metadata_not_found(storage_provider):
    """Test that get_download_metadata returns None for missing objects."""
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

    metadata = await storage_provider.get_download_metadata(not_found_oid)
    assert metadata is None


@pytest.mark.asyncio
async def test_managed_exists_with_metadata(storage_object, mock_lfs_server):
    """Test managed_exists returns True when LFS object exists."""
    inject_pointer(storage_object.provider, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))

    mock_client = MagicMock()
    mock_client.post = mock_lfs_server["mock_post"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_object.provider.client = mock_client_ctx

    assert await storage_object.managed_exists() is True


@pytest.mark.asyncio
async def test_managed_mtime_is_zero(storage_object):
    """LFS objects are immutable so mtime is always 0."""
    assert await storage_object.managed_mtime() == 0


@pytest.mark.asyncio
async def test_managed_size(storage_object):
    """Test that managed_size returns the size from the pointer metadata."""
    inject_pointer(storage_object.provider, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))

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

    inject_pointer(storage_object.provider, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))
    inject_download_metadata(storage_object.provider, TEST_OID, TEST_DOWNLOAD_URL, len(TEST_CONTENT))

    mock_client = MagicMock()
    mock_client.stream = mock_lfs_server["mock_stream"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_object.provider.client = mock_client_ctx

    await storage_object.managed_retrieve()

    assert local_path.exists()
    assert local_path.read_bytes() == TEST_CONTENT

    storage_object.verify_checksum(local_path, TEST_OID)


def test_wrong_checksum_detection(storage_object, tmp_path):
    """Test that corrupted files are detected via checksum."""
    corrupted_path = tmp_path / "corrupted.json"
    corrupted_path.write_bytes(b'{"corrupted": "data"}')

    with pytest.raises(WrongChecksum):
        storage_object.verify_checksum(corrupted_path, TEST_OID)


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

    inject_pointer(storage_provider_with_cache, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))
    inject_download_metadata(storage_provider_with_cache, TEST_OID, TEST_DOWNLOAD_URL, len(TEST_CONTENT))

    mock_client = MagicMock()
    mock_client.stream = mock_lfs_server["mock_stream"]

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    storage_provider_with_cache.client = mock_client_ctx

    await obj1.managed_retrieve()

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
async def test_local_lfs_object_lookup(tmp_path, test_logger):
    """Test that LFS objects are found in the local repo's LFS object store."""
    repo_dir = tmp_path / "repo"
    lfs_blob = repo_dir / "lfs" / "objects" / TEST_OID[:2] / TEST_OID[2:4] / TEST_OID
    lfs_blob.parent.mkdir(parents=True)
    lfs_blob.write_bytes(TEST_CONTENT)

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

    assert provider.local_git.find_lfs_object(TEST_OID) == lfs_blob

    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )
    inject_pointer(provider, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))

    local_path = tmp_path / "out" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    with assert_no_http_requests(provider):
        await obj.managed_retrieve()

    assert local_path.read_bytes() == TEST_CONTENT


@pytest.mark.asyncio
async def test_regular_file_retrieve(storage_provider, tmp_path):
    """Test that regular (non-LFS) git files are written directly."""
    import tempfile

    tmp = tempfile.NamedTemporaryFile(delete=True)
    tmp.write(TEST_CONTENT)
    tmp.flush()

    storage_provider.pointer_cache[(TEST_REF, TEST_PATH)] = PointerMetadata(
        oid=None, size=len(TEST_CONTENT), tmp_file=tmp
    )

    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )
    local_path = tmp_path / "out" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    with assert_no_http_requests(storage_provider):
        await obj.managed_retrieve()

    assert local_path.read_bytes() == TEST_CONTENT


def make_mock_client(content: bytes, fail_at: int | None, oid: str):
    """
    Factory for a mock httpx client simulating a range-capable HTTP server.

    Args:
        content: The full file content to serve.
        fail_at: If set, drop the connection after serving this many bytes on the
            first (non-Range) request, simulating a mid-transfer interruption.
            If None, serve the full content without interruption.
        oid: Expected OID (used to build a valid batch API response).
    """
    received_range_headers: list[str | None] = []

    async def mock_post(url, **kwargs):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = make_lfs_batch_response(oid, TEST_DOWNLOAD_URL, len(content))
        return resp

    @asynccontextmanager
    async def mock_stream(method, url, **kwargs):
        range_header = (kwargs.get("headers") or {}).get("Range")
        received_range_headers.append(range_header)

        resp = MagicMock()
        if range_header is None:
            resp.status_code = 200
            chunk = content
            drop_at = fail_at
        else:
            resp.status_code = 206
            offset = int(range_header.removeprefix("bytes=").removesuffix("-"))
            chunk = content[offset:]
            drop_at = None

        async def aiter_bytes(chunk_size=8192):
            if drop_at is None:
                yield chunk
            else:
                yield chunk[:drop_at]
                raise ConnectionError("peer closed connection")

        resp.aiter_bytes = aiter_bytes
        resp.headers = {"content-length": str(len(chunk))}
        yield resp

    mock_client = MagicMock()
    mock_client.post = mock_post
    mock_client.stream = mock_stream

    @asynccontextmanager
    async def mock_client_ctx():
        yield mock_client

    mock_client_ctx.received_range_headers = received_range_headers
    return mock_client_ctx


@pytest.mark.asyncio
async def test_resume_on_partial_file(storage_provider, tmp_path):
    """Test that downloads resume from partial files using HTTP Range requests."""
    fail_at = 10
    mock_client_ctx = make_mock_client(TEST_CONTENT, fail_at=fail_at, oid=TEST_OID)
    storage_provider.client = mock_client_ctx

    inject_pointer(storage_provider, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))

    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=storage_provider,
    )

    local_path = tmp_path / "resume_test" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    await obj.managed_retrieve()

    assert len(mock_client_ctx.received_range_headers) == 2
    assert mock_client_ctx.received_range_headers[0] is None  # first attempt: no Range header
    assert mock_client_ctx.received_range_headers[1] == f"bytes={fail_at}-"  # resume from interruption point
    assert local_path.read_bytes() == TEST_CONTENT


@pytest.mark.asyncio
async def test_local_repo_oid_mismatch_raises(tmp_path, test_logger):
    """Test that a local LFS object with wrong content raises a WorkflowError."""
    wrong_content = b"this is wrong content"

    repo_dir = tmp_path / "repo"
    lfs_blob = repo_dir / "lfs" / "objects" / TEST_OID[:2] / TEST_OID[2:4] / TEST_OID
    lfs_blob.parent.mkdir(parents=True)
    lfs_blob.write_bytes(wrong_content)

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
    inject_pointer(provider, TEST_REF, TEST_PATH, TEST_OID, len(TEST_CONTENT))

    obj = StorageObject(
        query=TEST_URL,
        keep_local=False,
        retrieve=True,
        provider=provider,
    )

    local_path = tmp_path / "out" / "test.json"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    obj.local_path = lambda: local_path

    with pytest.raises(WorkflowError, match="unexpected content"):
        await obj.managed_retrieve()
