# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import base64
import hashlib
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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
from .common import link_or_copy
from .git_api import GitApiProvider, LocalGitProvider, PointerMetadata

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
            "help": "Git repository URL used to construct the LFS batch API endpoint and resolve file pointers (e.g. https://github.com/org/repo).",
            "env_var": True,
        },
    )
    token_envvar: str = field(
        default="",
        metadata={
            "help": "Name of the environment variable containing the authentication token for the git host and LFS server.",
            "env_var": False,
        },
    )
    local_repo: str = field(
        default="",
        metadata={
            "help": "Path to a local git repository. Used to resolve file pointers and look up LFS objects before downloading.",
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


def utc_after(hours: float = 0, minutes: float = 0, seconds: float = 0) -> datetime:
    return datetime.now(timezone.utc) + timedelta(
        hours=hours, minutes=minutes, seconds=seconds
    )


@dataclass
class DownloadMetadata:
    """LFS batch API response for a single object."""

    url: str
    headers: dict[str, str]
    expires_at: datetime


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
    token: str | None
    local_git: LocalGitProvider | None
    remote_git: GitApiProvider

    pointer_cache: dict[tuple[str, str], PointerMetadata | None]
    download_cache: dict[str, DownloadMetadata]

    def __post_init__(self):
        super().__post_init__()

        if not self.settings.repo_url:
            raise WorkflowError(
                "repo_url must be set in StorageProviderSettings to use the LFS storage plugin"
            )

        self.cache = (
            Cache(cache_dir=Path(self.settings.cache)) if self.settings.cache else None
        )

        self._client: httpx.AsyncClient | None = None
        self._client_refcount: int = 0

        if self.settings.token_envvar:
            token = os.environ.get(self.settings.token_envvar) or None
            if token is None:
                raise WorkflowError(
                    f"token_envvar is set to '{self.settings.token_envvar}' "
                    f"but that environment variable is not set or empty."
                )
        else:
            token = None
        self.token = token

        self.local_git = (
            LocalGitProvider(Path(self.settings.local_repo).expanduser())
            if self.settings.local_repo
            else None
        )
        self.remote_git = GitApiProvider.from_repo_url(
            self.settings.repo_url, token=token
        )

        self.pointer_cache = {}
        self.download_cache = {}

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
                query="lfs://v1.2.3/path/to/file.csv",
                description="A file in a Git LFS repository at a given tag",
                type=QueryType.INPUT,
            ),
            ExampleQuery(
                query="lfs://abc1234/path/to/file.csv",
                description="A file in a Git LFS repository at a given commit",
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
            self._client = httpx.AsyncClient(
                follow_redirects=True, limits=limits, timeout=timeout
            )

        try:
            yield self._client
        finally:
            self._client_refcount -= 1
            if self._client_refcount == 0:
                await self._client.aclose()
                self._client = None

    def _lfs_batch_api_url(self) -> str:
        repo_url = self.settings.repo_url.rstrip("/")
        return f"{repo_url}.git/info/lfs/objects/batch"

    def _lfs_auth_headers(self) -> dict[str, str]:
        if self.token is None:
            return {}
        encoded = base64.b64encode(f"git:{self.token}".encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    async def get_pointer_metadata(self, ref: str, path: str) -> PointerMetadata | None:
        key = (ref, path)
        if key in self.pointer_cache:
            return self.pointer_cache[key]

        if self.local_git and (
            meta := await self.local_git.get_pointer_metadata(ref, path)
        ):
            self.pointer_cache[key] = meta
            return meta

        async with self.client() as client:
            meta = await self.remote_git.get_pointer_metadata(client, ref, path)
        self.pointer_cache[key] = meta
        return meta

    @retry_decorator
    async def get_download_metadata(self, oid: str) -> DownloadMetadata | None:
        if oid in self.download_cache:
            cached = self.download_cache[oid]
            if cached.expires_at >= utc_after(minutes=5):
                return cached

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
                        **self._lfs_auth_headers(),
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

        if "error" in obj:
            err = obj["error"]
            code = err.get("code", "?")
            msg = err.get("message", "")
            if code == 404:
                return None
            raise WorkflowError(f"LFS batch API error for OID {oid}: {code} {msg}")

        actions = obj.get("actions", {})
        download = actions.get("download", {})
        download_url: str | None = download.get("href")
        download_headers: dict[str, str] = download.get("header", {})

        if not download_url:
            return None

        if expires_in := download.get("expires_in"):
            expires_at = utc_after(seconds=float(expires_in))
        elif expires_at_str := download.get("expires_at"):
            expires_at = datetime.fromisoformat(expires_at_str).astimezone(timezone.utc)
        else:
            expires_at = utc_after(hours=1)

        metadata = DownloadMetadata(
            url=download_url,
            headers=download_headers,
            expires_at=expires_at,
        )

        self.download_cache[oid] = metadata
        return metadata


class StorageObject(StorageObjectRead):
    provider: StorageProvider  # pyright: ignore[reportIncompatibleVariableOverride]
    ref: str
    lfs_path: str

    def __post_init__(self):
        super().__post_init__()

        parsed = urlparse(str(self.query))
        if parsed.scheme != "lfs":
            raise WorkflowError(f"Invalid LFS URL scheme: {self.query}")

        self.ref = parsed.netloc
        self.lfs_path = parsed.path.strip("/")

        if not self.ref:
            raise WorkflowError(
                f"Invalid LFS URL: {self.query}. Expected format: lfs://{{ref}}/{{path}}"
            )

    @override
    def local_suffix(self) -> str:
        return f"{self.ref}/{self.lfs_path}"

    @override
    def get_inventory_parent(self) -> str | None:
        return None

    @override
    async def managed_exists(self) -> bool:
        if self.provider.settings.skip_remote_checks:
            return True

        return (
            await self.provider.get_pointer_metadata(self.ref, self.lfs_path)
            is not None
        )

    @override
    async def managed_mtime(self) -> float:
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

        pointer = await self.provider.get_pointer_metadata(self.ref, self.lfs_path)
        return pointer.size if pointer is not None else 0

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

        pointer = await self.provider.get_pointer_metadata(self.ref, self.lfs_path)
        cache.exists_in_storage[key] = pointer is not None
        cache.mtime[key] = Mtime(storage=0)
        cache.size[key] = pointer.size if pointer is not None else 0

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

    def verify_checksum(self, path: Path, oid: str) -> None:
        with open(path, "rb") as f:
            observed = hashlib.file_digest(f, "sha256").hexdigest().lower()
        expected = oid.lower()
        if expected != observed:
            raise WrongChecksum(observed=observed, expected=expected)

    @retry_decorator
    async def managed_retrieve(self):
        local_path = self.local_path()
        local_path.parent.mkdir(parents=True, exist_ok=True)

        filename = self.lfs_path.split("/")[-1] if self.lfs_path else self.ref[:12]
        query = str(self.query)

        pointer = await self.provider.get_pointer_metadata(self.ref, self.lfs_path)
        if pointer is None:
            raise WorkflowError(
                f"File not found in repository: {self.lfs_path!r} at {self.ref!r}"
            )

        # Regular file (not LFS)
        if pointer.oid is None:
            assert pointer.tmp_path is not None
            link_or_copy(pointer.tmp_path, local_path, may_symlink=False)
            if self.provider.cache:
                self.provider.cache.put(query, local_path)
            logger.info(f"Retrieved {filename} (regular git file)")
            return

        oid = pointer.oid

        # 1. Try local LFS object store
        if self.provider.local_git:
            local_lfs = self.provider.local_git.find_lfs_object(oid)
            if local_lfs is not None:
                try:
                    self.verify_checksum(local_lfs, oid)
                except WrongChecksum as e:
                    raise WorkflowError(
                        f"Local LFS object for {self.lfs_path!r} has unexpected content:\n"
                        f"  Expected OID: {e.expected}\n"
                        f"  Found OID:    {e.observed}"
                    ) from e
                link_or_copy(local_lfs, local_path)
                logger.info(f"Retrieved {filename} from local repo ({oid[:12]})")
                return

        # 2. Try cache
        if self.provider.cache:
            cached = self.provider.cache.get(query)
            if cached is not None:
                try:
                    self.verify_checksum(cached, oid)
                    link_or_copy(cached, local_path)
                    logger.info(f"Retrieved {filename} from cache ({oid[:12]})")
                    return
                except WrongChecksum as e:
                    logger.warning(
                        f"Cached file has unexpected checksum: expected {e.expected}, "
                        f"got {e.observed}. Discarding cache entry and downloading."
                    )
                    cached.unlink(missing_ok=True)

        # 3. Fetch download URL from LFS batch API
        download = await self.provider.get_download_metadata(oid)
        if download is None:
            raise WorkflowError(f"LFS object not found: {oid}")

        # Check for existing partial file to resume
        offset = local_path.stat().st_size if local_path.exists() else 0
        headers = dict(download.headers)
        if offset > 0:
            headers["Range"] = f"bytes={offset}-"

        try:
            async with (
                self.provider.client() as client,
                client.stream("get", download.url, headers=headers) as response,
            ):
                if response.status_code == 206:
                    mode = "ab"
                    logger.info(f"Resuming {filename} from byte {offset}")
                elif response.status_code == 200:
                    mode = "wb"
                    offset = 0
                elif response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        f"Failed to download LFS object: HTTP {response.status_code} ({download.url})",
                        request=response.request,
                        response=response,
                    )
                else:
                    raise WorkflowError(
                        f"Failed to download LFS object: HTTP {response.status_code} ({download.url})"
                    )

                total_size = int(response.headers.get("content-length", 0)) + offset

                with local_path.open(mode=mode) as f:
                    with tqdm(
                        total=total_size,
                        initial=offset,
                        unit="B",
                        unit_scale=True,
                        desc=filename,
                        position=None,
                        leave=True,
                    ) as pbar:
                        async for chunk in response.aiter_bytes(chunk_size=8192):
                            f.write(chunk)
                            pbar.update(len(chunk))

            self.verify_checksum(local_path, oid)
            logger.info(f"Retrieved {filename} from remote ({oid[:12]})")

            if self.provider.cache:
                self.provider.cache.put(query, local_path)

        except (TimeoutError, ConnectionError, httpx.TransportError):
            # Mid-transfer interruption — keep partial file for resume on next retry
            raise
        except:
            if local_path.exists():
                local_path.unlink()
            raise
