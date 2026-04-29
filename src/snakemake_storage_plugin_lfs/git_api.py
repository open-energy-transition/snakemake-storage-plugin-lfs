# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import os
import subprocess
import tempfile
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO
from urllib.parse import urlparse

import httpx
from snakemake_interface_common.exceptions import WorkflowError

LFS_POINTER_HEADER = b"version https://git-lfs.github.com/spec/v1\n"


@dataclass
class PointerMetadata:
    """Resolved metadata for a file at a given git ref and path."""

    oid: str | None  # sha256 hex; empty string for regular (non-LFS) files
    size: int
    tmp_file: IO[bytes] | None = field(default=None, repr=False)


def _parse_blob(raw: bytes, path: str) -> PointerMetadata:
    if raw.startswith(LFS_POINTER_HEADER):
        oid = None
        size = None
        for line in raw.decode().splitlines():
            if line.startswith("oid sha256:"):
                oid = line.split(":", 1)[1].strip()
            elif line.startswith("size "):
                size = int(line.split(" ", 1)[1].strip())
        if not oid or size is None:
            raise WorkflowError(f"Malformed LFS pointer: {raw[:200]!r}")
        return PointerMetadata(oid=oid, size=size)

    tmp = tempfile.NamedTemporaryFile(
        suffix="_" + os.path.basename(path), delete_on_close=False
    )
    with tmp:
        tmp.write(raw)
    return PointerMetadata(oid=None, size=len(raw), tmp_file=tmp)


@dataclass
class LocalGitProvider:
    repo_path: Path

    async def get_pointer_metadata(self, ref: str, path: str) -> PointerMetadata | None:
        result = subprocess.run(
            ["git", "cat-file", "blob", f"{ref}:{path}"],
            cwd=self.repo_path,
            capture_output=True,
        )
        if result.returncode != 0:
            return None
        return _parse_blob(result.stdout, path=path)

    def find_lfs_object(self, oid: str) -> Path | None:
        """Look up an LFS object by OID in a local repo's LFS object store."""
        for lfs_root in [self.repo_path / "lfs", self.repo_path / ".git" / "lfs"]:
            lfs_blob = lfs_root / "objects" / oid[:2] / oid[2:4] / oid
            if lfs_blob.exists():
                return lfs_blob


class GitApiProvider(ABC):
    token: str | None

    @abstractmethod
    async def get_pointer_metadata(
        self, client: httpx.AsyncClient, ref: str, path: str
    ) -> PointerMetadata | None:
        """Fetch and parse the blob at ref:path into PointerMetadata, or None if not found."""
        ...

    @staticmethod
    def from_repo_url(repo_url: str, token: str | None = None) -> "GitApiProvider":
        parsed = urlparse(repo_url)
        if parsed.hostname == "github.com":
            return GitHubApiProvider(repo_url=repo_url, token=token)
        return GitLabApiProvider(repo_url=repo_url, token=token)

    def _auth_headers(self) -> dict[str, str]:
        if not self.token:
            return {}
        return {"Authorization": f"Bearer {self.token}"}


@dataclass
class GitHubApiProvider(GitApiProvider):
    repo_url: str
    token: str | None = None

    async def get_pointer_metadata(
        self, client: httpx.AsyncClient, ref: str, path: str
    ) -> PointerMetadata | None:
        owner, repo = urlparse(self.repo_url).path.strip("/").split("/", 1)
        url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"
        resp = await client.get(
            url,
            headers={
                "Accept": "application/vnd.github.raw+json",
                **self._auth_headers(),
            },
        )
        if resp.status_code == 404:
            return None
        if resp.status_code >= 500:
            raise httpx.HTTPStatusError(
                f"GitHub API returned HTTP {resp.status_code} for {path!r} at {ref!r} ({url})",
                request=resp.request,
                response=resp,
            )
        if resp.status_code != 200:
            raise WorkflowError(
                f"GitHub API returned HTTP {resp.status_code} for {path!r} at {ref!r} ({url})"
            )
        return _parse_blob(resp.content, path)


@dataclass
class GitLabApiProvider(GitApiProvider):
    repo_url: str
    token: str | None = None

    async def get_pointer_metadata(
        self, client: httpx.AsyncClient, ref: str, path: str
    ) -> PointerMetadata | None:
        parsed = urlparse(self.repo_url)
        project = parsed.path.strip("/").replace("/", "%2F")
        encoded_path = path.replace("/", "%2F")
        url = f"{parsed.scheme}://{parsed.netloc}/api/v4/projects/{project}/repository/files/{encoded_path}/raw?ref={ref}"
        resp = await client.get(url, headers=self._auth_headers())
        if resp.status_code == 404:
            return None
        if resp.status_code >= 500:
            raise httpx.HTTPStatusError(
                f"GitLab API returned HTTP {resp.status_code} for {path!r} at {ref!r} ({url})",
                request=resp.request,
                response=resp,
            )
        if resp.status_code != 200:
            raise WorkflowError(
                f"GitLab API returned HTTP {resp.status_code} for {path!r} at {ref!r} ({url})"
            )
        return _parse_blob(resp.content, path)
