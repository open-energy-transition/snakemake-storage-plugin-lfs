<!--
SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
SPDX-License-Identifier: CC-BY-4.0
-->

# Snakemake Storage Plugin: LFS

A Snakemake storage plugin for downloading files from Git LFS (Large File Storage) servers with optional local caching.

## Features

- **Git LFS protocol**: Fetches objects via the [Git LFS Batch API](https://github.com/git-lfs/git-lfs/blob/main/docs/api/batch.md)
- **Local repo lookup**: Checks a local git repository's LFS store before downloading remotely
- **Local caching**: Downloaded objects can be cached to avoid redundant transfers
- **Checksum verification**: Verifies SHA-256 integrity (the LFS OID *is* the SHA-256 digest)
- **Authentication**: Supports token-based Basic Auth via environment variable
- **Concurrent download control**: Limits simultaneous downloads
- **Progress bars**: Shows download progress with tqdm
- **Immutable objects**: Returns mtime=0 (LFS objects are content-addressed and never change)
- **Environment variable support**: Configure via environment variables for CI/CD

## Installation

```bash
pip install snakemake-storage-plugin-lfs
```

## URL Format

LFS objects are referenced using the `lfs://` scheme:

```
lfs://{oid}/{path}
```

- `{oid}` — SHA-256 hex digest of the object (64 hex characters)
- `{path}` — logical file path used as the local filename

**Example:**

```
lfs://3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4/data/natura.tiff
```

## Configuration

Register the plugin in your Snakefile:

```python
storage lfs:
    provider="lfs",
    repo_url="https://github.com/org/repo",  # required
```

### Settings

| Setting | Default | Env var | Description |
|---------|---------|---------|-------------|
| `repo_url` | `""` | `SNAKEMAKE_STORAGE_LFS_REPO_URL` | Git repository URL used to construct the LFS Batch API endpoint (e.g. `https://github.com/org/repo`). **Required.** |
| `token_envvar` | `""` | — | Name of the environment variable containing the authentication token (used as Basic Auth password with username `git`). |
| `local_repo` | `""` | `SNAKEMAKE_STORAGE_LFS_LOCAL_REPO` | Path to a local git repository. Files are looked up by path in the working tree before downloading remotely. If found but the SHA-256 hash does not match the OID, a warning is issued and the remote is used. |
| `cache` | `""` | `SNAKEMAKE_STORAGE_LFS_CACHE` | Path to a cache directory for downloaded objects. Set to a path to enable caching; leave empty to disable. |
| `skip_remote_checks` | `False` | `SNAKEMAKE_STORAGE_LFS_SKIP_REMOTE_CHECKS` | Skip existence/size checks against the remote LFS server. Useful in CI/CD when inputs are known to exist. |
| `max_concurrent_downloads` | `3` | — | Maximum number of simultaneous downloads. |

## Usage

Use `lfs://` URLs directly in your rules:

```python
rule use_lfs_file:
    input:
        storage.lfs(
            "lfs://3b4c5d6e7f8a9b0c1d2e3f4a5b6c7d8e9f0a1b2c3d4e5f6a7b8c9d0e1f2a3b4/data/natura.tiff"
        ),
    output:
        "resources/natura.tiff"
    shell:
        "cp {input} {output}"
```

The plugin will:
1. Check the local git repository's LFS store (if `local_repo` is set)
2. Check the local cache (if `cache` is set)
3. If not found locally, query the LFS Batch API for a download URL
4. Download the object with a progress bar
5. Verify the SHA-256 checksum against the OID
6. Store in the cache (if `cache` is set)

### Authentication

To access private repositories, set `token_envvar` to the name of an environment variable that holds the token:

```python
storage lfs:
    provider="lfs",
    repo_url="https://github.com/org/private-repo",
    token_envvar="GITHUB_TOKEN",
```

```bash
export GITHUB_TOKEN="ghp_..."
snakemake --cores all
```

### CI/CD Configuration

```yaml
# GitHub Actions example
- name: Run snakemake workflows
  env:
    SNAKEMAKE_STORAGE_LFS_REPO_URL: "https://github.com/org/repo"
    SNAKEMAKE_STORAGE_LFS_SKIP_REMOTE_CHECKS: "1"
    GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
  run: |
    snakemake --cores all
```

## How LFS Objects Are Located

Priority order in `managed_retrieve()`:

1. **Local repo** (`local_repo` setting): Looks up the file by its path in the working tree (`{local_repo}/{lfs_path}`). The SHA-256 hash is verified against the OID; on mismatch a warning is issued and download proceeds.
2. **Cache** (`cache` setting): Checks the configured cache directory.
3. **Remote**: Queries the LFS Batch API (`{repo_url}.git/info/lfs/objects/batch`) and downloads from the returned URL.

## License

MIT License
