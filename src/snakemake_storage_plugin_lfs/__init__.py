# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import hashlib
import os
import shutil
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from functools import cached_property
from logging import Logger
from pathlib import Path
from urllib.parse import urlparse

import httpx
from reretry import retry  # pyright: ignore[reportUnknownVariableType]
from snakemake_interface_common.exceptions import WorkflowError
from snakemake_interface_common.logging import get_logger
from snakemake_interface_common.plugin_registry.plugin import SettingsBase
from snakemake_interface_storage_plugins.common import Operation
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface, Mtime
from snakemake_interface_storage_plugins.storage_object import StorageObjectRead
from snakemake_interface_storage_plugins.storage_provider import (
    ExampleQuery,
    QueryType,
    StorageProviderBase,
    StorageQueryValidationResult,
)
from tqdm_loggable.auto import tqdm
from typing_extensions import override

from .cache import Cache

logger = get_logger()


class ReretryLoggerAdapter:
    """Adapter to make Snakemake's logger compatible with reretry's logging expectations."""

    _logger: Logger

    def __init__(self, snakemake_logger: Logger):
        self._logger = snakemake_logger

    def warning(self, msg: str, *args, **kwargs):  # pyright: ignore[reportUnknownParameterType, reportUnusedParameter, reportMissingParameterType]
        """
        Format message manually before passing to Snakemake logger.

        This is necessary because Snakemake's DefaultFormatter has a bug where it
        returns record["msg"] without calling interpolating the args. This causes
        literal "%s" to appear in log output instead of formatted values.
        """
        if args:
            msg = msg % args
        self._logger.warning(msg)


@dataclass
class StorageProviderSettings(SettingsBase):
    repo_url: str = field(
        default="",
        metadata={
            "help": "Git repository URL used to construct the LFS batch API endpoint (e.g. https://github.com/org/repo).",
            "env_var": True,
        },
    )
    token_envvar: str = field(
        default="",
        metadata={
            "help": "Name of the environment variable containing the authentication token for the LFS server (used as Basic Auth password).",
            "env_var": False,
        },
    )
    local_repo: str = field(
        default="",
        metadata={
            "help": "Path to a local git repository to look up LFS objects before downloading. If the OID is found locally but the hash does not match, a warning is issued.",
            "env_var": True,
        },
    )
    cache: str = field(
        default="",
        metadata={
            "help": 'Cache directory for downloaded files. Set to a path to enable caching (default: "" = disabled).',
            "env_var": True,
        },
    )
    skip_remote_checks: bool = field(
        default=False,
        metadata={
            "help": "Whether to skip metadata checking with remote LFS server (default: False).",
            "env_var": True,
        },
    )
    max_concurrent_downloads: int = field(
        default=3,
        metadata={
            "help": "Maximum number of concurrent downloads.",
            "env_var": False,
        },
    )


@dataclass
class FileMetadata:
    """Metadata for a file from the LFS batch API."""

    checksum: str | None  # "sha256:{hexdigest}"
    size: int
    download_url: str | None = None
    download_headers: dict[str, str] = field(default_factory=dict)


class WrongChecksum(Exception):
    observed: str
    expected: str

    def __init__(self, observed: str, expected: str):
        self.observed = observed
        self.expected = expected
        super().__init__(f"Checksum mismatch: expected {expected}, got {observed}")


retry_decorator = retry(
    exceptions=(  # pyright: ignore[reportArgumentType]
        httpx.HTTPError,
        TimeoutError,
        OSError,
        WrongChecksum,
    ),
    tries=5,
    delay=3,
    backoff=2,
    logger=ReretryLoggerAdapter(get_logger()),  # pyright: ignore[reportArgumentType]
)


def is_lfs_url(query: str) -> bool:
    parsed = urlparse(query)
    return parsed.scheme == "lfs"


class StorageProvider(StorageProviderBase):
    settings: StorageProviderSettings
    cache: Cache | None

    def __post_init__(self):
        super().__post_init__()

        self.cache = (
            Cache(cache_dir=Path(self.settings.cache)) if self.settings.cache else None
        )

        self._client: httpx.AsyncClient | None = None
        self._client_refcount: int = 0

        # Cache for LFS batch API results: oid -> FileMetadata
        self._lfs_metadata_cache: dict[str, FileMetadata] = {}

    @override
    def use_rate_limiter(self) -> bool:
        return False

    @override
    def rate_limiter_key(self, query: str, operation: Operation) -> str:
        raise NotImplementedError()

    @override
    def default_max_requests_per_second(self) -> float:
        raise NotImplementedError()

    @override
    @classmethod
    def example_queries(cls) -> list[ExampleQuery]:
        return [
            ExampleQuery(
                query="lfs://abc123def456/path/to/file.csv",
                description="A Git LFS object by OID and path",
                type=QueryType.INPUT,
            ),
        ]

    @override
    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        if is_lfs_url(query):
            return StorageQueryValidationResult(query=query, valid=True)

        return StorageQueryValidationResult(
            query=query,
            valid=False,
            reason="Only lfs:// URLs are handled by this plugin",
        )

    @override
    @classmethod
    def get_storage_object_cls(cls):
        return StorageObject

    @asynccontextmanager
    async def client(self):
        """Reentrant async context manager for httpx.AsyncClient."""
        self._client_refcount += 1

        if self._client is None:
            max_concurrent_downloads = self.settings.max_concurrent_downloads
            limits = httpx.Limits(
                max_keepalive_connections=max_concurrent_downloads,
                max_connections=max_concurrent_downloads,
            )
            timeout = httpx.Timeout(60, pool=None)

            auth = None
            if self.settings.token_envvar:
                token = os.environ.get(self.settings.token_envvar, "")
                if not token:
                    raise WorkflowError(
                        f"token_envvar is set to '{self.settings.token_envvar}' "
                        f"but that environment variable is not set or empty."
                    )
                auth = httpx.BasicAuth(username="git", password=token)

            self._client = httpx.AsyncClient(
                follow_redirects=True, limits=limits, timeout=timeout, auth=auth
            )

        try:
            yield self._client
        finally:
            self._client_refcount -= 1
            if self._client_refcount == 0:
                await self._client.aclose()
                self._client = None

    def _lfs_batch_api_url(self) -> str:
        """Construct the LFS batch API URL from repo_url."""
        repo_url = self.settings.repo_url.rstrip("/")
        if not repo_url:
            raise WorkflowError(
                "repo_url must be set in StorageProviderSettings to use the LFS storage plugin"
            )
        return f"{repo_url}.git/info/lfs/objects/batch"

    @retry_decorator
    async def get_metadata(self, oid: str) -> FileMetadata | None:
        """
        Retrieve file metadata via the Git LFS Batch API.

        Args:
            oid: Git LFS object ID (SHA-256 hex digest)

        Returns:
            FileMetadata with download URL, headers, checksum, and size
        """
        if oid in self._lfs_metadata_cache:
            return self._lfs_metadata_cache[oid]

        batch_url = self._lfs_batch_api_url()
        payload = {
            "operation": "download",
            "transfers": ["basic"],
            "objects": [{"oid": oid, "size": 0}],
        }

        try:
            async with self.client() as client:
                response = await client.post(
                    batch_url,
                    json=payload,
                    headers={
                        "Accept": "application/vnd.git-lfs+json",
                        "Content-Type": "application/vnd.git-lfs+json",
                    },
                )
        except Exception as e:
            logger.warning(
                f"{type(e).__name__} while querying LFS batch API at {batch_url}"
            )
            raise

        if response.status_code >= 500:
            raise httpx.HTTPStatusError(
                f"LFS batch API returned HTTP {response.status_code} for OID {oid} ({batch_url})",
                request=response.request,
                response=response,
            )
        if response.status_code != 200:
            raise WorkflowError(
                f"LFS batch API returned HTTP {response.status_code} for OID {oid} ({batch_url})"
            )

        data = response.json()
        objects = data.get("objects", [])
        if not objects:
            return None

        obj = objects[0]

        # Check for error in object response
        if "error" in obj:
            err = obj["error"]
            code = err.get("code", "?")
            msg = err.get("message", "")
            if code == 404:
                return None
            raise WorkflowError(f"LFS batch API error for OID {oid}: {code} {msg}")

        size: int = obj.get("size", 0)
        actions = obj.get("actions", {})
        download = actions.get("download", {})
        download_url: str | None = download.get("href")
        download_headers: dict[str, str] = download.get("header", {})

        metadata = FileMetadata(
            checksum=f"sha256:{oid}",
            size=size,
            download_url=download_url,
            download_headers=download_headers,
        )

        self._lfs_metadata_cache[oid] = metadata
        return metadata

    def local_repo_path(self) -> Path | None:
        """Return the resolved local repo path, or None if not configured."""
        local_repo = self.settings.local_repo
        if not local_repo:
            return None
        return Path(local_repo).expanduser()


class StorageObject(StorageObjectRead):
    provider: StorageProvider  # pyright: ignore[reportIncompatibleVariableOverride]
    oid: str
    lfs_path: str

    def __post_init__(self):
        super().__post_init__()

        # Parse lfs://{oid}/{path}
        parsed = urlparse(str(self.query))
        if parsed.scheme != "lfs":
            raise WorkflowError(f"Invalid LFS URL scheme: {self.query}")

        # netloc is the oid, path is the file path
        self.oid = parsed.netloc
        self.lfs_path = parsed.path.strip("/")

        if not self.oid:
            raise WorkflowError(
                f"Invalid LFS URL: {self.query}. Expected format: lfs://{{oid}}/{{path}}"
            )

    @override
    def local_suffix(self) -> str:
        """Return the local suffix for this object (used by parent class)."""
        return f"{self.oid}/{self.lfs_path}"

    @override
    def get_inventory_parent(self) -> str | None:
        return None

    @cached_property
    def local_repo_file(self) -> Path | None:
        """
        Look up the checked-out file in the local git repository's working tree.

        Returns the path if the file exists and is not an LFS pointer stub.
        Callers should verify the checksum themselves.
        """
        repo_path = self.provider.local_repo_path()
        if repo_path is None:
            return None
        candidate = repo_path / self.lfs_path
        if not candidate.exists():
            return None
        # Skip LFS pointer stubs (file not yet pulled)
        with candidate.open("rb") as f:
            if f.read(43) == b"version https://git-lfs.github.com/spec/v1\n":
                logger.warning(
                    f"Skipping LFS pointer stub in local repo: {self.lfs_path}"
                )
                return None
        return candidate

    @override
    async def managed_exists(self) -> bool:
        if self.provider.settings.skip_remote_checks:
            return True

        # Check local repo first
        if self.local_repo_file is not None:
            return True

        # Check cache
        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                return True

        metadata = await self.provider.get_metadata(self.oid)
        return metadata is not None

    @override
    async def managed_mtime(self) -> float:
        # LFS objects are immutable (content-addressed), always return 0
        return 0

    @override
    async def managed_size(self) -> int:
        if self.provider.settings.skip_remote_checks:
            return 0

        # Check cache
        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                return cached.stat().st_size

        # Check local repo
        local_obj = self.local_repo_file
        if local_obj is not None:
            return local_obj.stat().st_size

        metadata = await self.provider.get_metadata(self.oid)
        return metadata.size if metadata is not None else 0

    @override
    async def inventory(self, cache: IOCacheStorageInterface) -> None:
        key = self.cache_key()
        if key in cache.exists_in_storage:
            return

        if self.provider.settings.skip_remote_checks:
            cache.exists_in_storage[key] = True
            cache.mtime[key] = Mtime(storage=0)
            cache.size[key] = 0
            return

        # Check local repo
        local_obj = self.local_repo_file
        if local_obj is not None:
            cache.exists_in_storage[key] = True
            cache.mtime[key] = Mtime(storage=0)
            cache.size[key] = local_obj.stat().st_size
            return

        # Check cache
        if self.provider.cache:
            cached = self.provider.cache.get(str(self.query))
            if cached is not None:
                cache.exists_in_storage[key] = True
                cache.mtime[key] = Mtime(storage=0)
                cache.size[key] = cached.stat().st_size
                return

        metadata = await self.provider.get_metadata(self.oid)
        if metadata is None:
            cache.exists_in_storage[key] = False
            cache.mtime[key] = Mtime(storage=0)
            cache.size[key] = 0
            return

        cache.exists_in_storage[key] = True
        cache.mtime[key] = Mtime(storage=0)
        cache.size[key] = metadata.size

    @override
    def cleanup(self):
        pass

    @override
    def exists(self) -> bool:
        raise NotImplementedError()

    @override
    def size(self) -> int:
        raise NotImplementedError()

    @override
    def mtime(self) -> float:
        raise NotImplementedError()

    @override
    def retrieve_object(self) -> None:
        raise NotImplementedError()

    def verify_checksum(self, path: Path) -> None:
        """
        Verify `path` against the expected SHA-256 checksum (the OID).

        Raises:
            WrongChecksum
        """
        with open(path, "rb") as f:
            checksum_observed = hashlib.file_digest(f, "sha256").hexdigest().lower()
        checksum_expected = self.oid.lower()

        if checksum_expected != checksum_observed:
            raise WrongChecksum(observed=checksum_observed, expected=checksum_expected)

    @retry_decorator
    async def managed_retrieve(self):
        """Async download with concurrency control, local repo lookup, caching, and checksum verification."""
        local_path = self.local_path()
        local_path.parent.mkdir(parents=True, exist_ok=True)

        query = str(self.query)
        filename = self.lfs_path.split("/")[-1] if self.lfs_path else self.oid[:12]

        # 1. Try local repo first
        local_file = self.local_repo_file
        if local_file is not None:
            try:
                self.verify_checksum(local_file)
            except WrongChecksum as e:
                raise WorkflowError(
                    f"Local repository file {self.lfs_path!r} exists, but has different "
                    f"content:\n"
                    f"  Expected OID: {e.expected}\n"
                    f"  Found OID:    {e.observed}"
                ) from e
            shutil.copy2(local_file, local_path)
            logger.info(f"Retrieved {filename} from local repo ({self.oid[:12]})")
            return

        # 2. Try cache
        if self.provider.cache:
            cached = self.provider.cache.get(query)
            if cached is not None:
                try:
                    self.verify_checksum(cached)
                    shutil.copy2(cached, local_path)
                    logger.info(f"Retrieved {filename} from cache ({self.oid[:12]})")
                    return
                except WrongChecksum as e:
                    logger.warning(
                        f"Cached file has unexpected checksum: expected {e.expected}, "
                        f"got {e.observed}. Discarding cache entry and downloading from remote."
                    )
                    # Cache entry was corrupt – remove it
                    cached.unlink(missing_ok=True)

        # 3. Fetch download URL from LFS batch API
        metadata = await self.provider.get_metadata(self.oid)
        if metadata is None:
            raise WorkflowError(f"LFS object not found: {self.oid}")

        download_url = metadata.download_url
        if not download_url:
            raise WorkflowError(
                f"No download URL returned by LFS batch API for OID {self.oid}"
            )

        try:
            async with (
                self.provider.client() as client,
                client.stream(
                    "get", download_url, headers=metadata.download_headers
                ) as response,
            ):
                if response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Failed to download LFS object: HTTP {response.status_code} ({download_url})",
                        request=response.request,
                        response=response,
                    )
                if response.status_code != 200:
                    raise WorkflowError(
                        f"Failed to download LFS object: HTTP {response.status_code} ({download_url})"
                    )

                total_size = int(response.headers.get("content-length", 0))

                with local_path.open(mode="wb") as f:
                    with tqdm(
                        total=total_size,
                        unit="B",
                        unit_scale=True,
                        desc=filename,
                        position=None,
                        leave=True,
                    ) as pbar:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))

            self.verify_checksum(local_path)
            logger.info(f"Retrieved {filename} from remote ({self.oid[:12]})")

            if self.provider.cache:
                self.provider.cache.put(query, local_path)

        except:
            if local_path.exists():
                local_path.unlink()
            raise
