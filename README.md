<!--
SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
SPDX-License-Identifier: CC-BY-4.0
-->

# Snakemake Storage Plugin: LFS

A Snakemake storage plugin for downloading files from Git LFS (Large File Storage) servers with optional local caching.

## Features

- **Git ref-based URLs**: Reference files by tag or commit
- **Regular file support**: Works for both LFS-tracked and regular git files at the given ref
- **Git API integration**: Resolves file pointers via the GitHub or GitLab API, or a local repository
- **Local repo lookup**: Checks a local git repository's LFS store before downloading remotely
- **Local caching**: Downloaded objects can be cached to avoid redundant transfers
- **Checksum verification**: Verifies SHA-256 integrity (the LFS OID *is* the SHA-256 digest)
- **Authentication**: Supports token-based auth via environment variable (Bearer for GitHub/GitLab API, Basic for LFS)
- **Concurrent download control**: Limits simultaneous downloads
- **Progress bars**: Shows download progress with tqdm
- **Immutable objects**: Returns mtime=0 (LFS objects are content-addressed and never change)
- **Environment variable support**: Configure via environment variables for CI/CD

## Installation

```bash
pip install snakemake-storage-plugin-lfs
```

## URL Format

Files are referenced using the `lfs://` scheme with a git ref:

```
lfs://{ref}/{path}
```

- `{ref}` — a fixed git ref: a tag (`v1.2.3`) or a full/short commit hash (`abc1234`)
- `{path}` — path to the file within the repository

Use only tags or commit hashes — **not branch names**. Branch names are mutable and would break Snakemake's assumption that a given input URL always refers to the same content.

**Examples:**

```
lfs://v1.2.3/data/natura.tiff
lfs://abc1234def/costs/v0.10.1/costs_2030.csv
```

The plugin resolves the file at the given ref using the GitHub or GitLab API (or a local repository), reads the LFS pointer to obtain the OID, and then downloads the object from the LFS server. Regular (non-LFS) files at the same ref are also supported and retrieved directly.

> **Breaking change from before v0.3:** The previous URL format `lfs://{oid}/{path}` (where `{oid}` was the 64-character SHA-256 LFS object ID) is no longer supported. Migrate by replacing the OID with the git ref (tag or commit) at which you want to pin the file.

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
| `repo_url` | `""` | `SNAKEMAKE_STORAGE_LFS_REPO_URL` | Git repository URL, used to query the API and construct the LFS Batch API endpoint (e.g. `https://github.com/org/repo`). **Required.** |
| `token_envvar` | `""` | — | Name of the environment variable containing the authentication token. Used as Bearer auth for the GitHub/GitLab API and as Basic auth for the LFS server. |
| `local_repo` | `""` | `SNAKEMAKE_STORAGE_LFS_LOCAL_REPO` | Path to a local git repository. Pointer resolution and LFS object lookup are attempted locally before falling back to the remote API. |
| `cache` | `""` | `SNAKEMAKE_STORAGE_LFS_CACHE` | Path to a cache directory for downloaded objects. Set to a path to enable caching; leave empty to disable. |
| `skip_remote_checks` | `False` | `SNAKEMAKE_STORAGE_LFS_SKIP_REMOTE_CHECKS` | Skip existence/size checks against the remote. Useful in CI/CD when inputs are known to exist. |
| `max_concurrent_downloads` | `3` | — | Maximum number of simultaneous downloads. |

## Usage

Use `lfs://` URLs directly in your rules:

```python
storage lfs:
    provider="lfs",
    repo_url="https://github.com/org/repo",
    token_envvar="GITHUB_TOKEN",

rule use_lfs_file:
    input:
        storage.lfs("lfs://v1.2.3/data/natura.tiff"),
    output:
        "resources/natura.tiff",
    shell:
        "cp {input} {output}"
```

The plugin will:
1. Resolve the file pointer at the given ref via the local repo or remote API
2. For regular files: retrieve content directly from git
3. For LFS files:
   - Check the local git repository's LFS store (if `local_repo` is set)
   - Check the local cache (if `cache` is set)
   - Query the LFS Batch API for a download URL
   - Download the object with a progress bar
   - Verify the SHA-256 checksum against the OID
   - Store in the cache (if `cache` is set)

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

The same token is used for both the GitHub/GitLab API (Bearer auth) and the LFS server (Basic auth with username `git`).

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

### Using a Local Repository

If you have a local clone of the repository, the plugin will resolve file pointers and look up LFS objects locally first, avoiding API calls:

```python
storage lfs:
    provider="lfs",
    repo_url="https://github.com/org/repo",
    local_repo="/path/to/local/clone",
```

Both bare repositories and working-tree clones are supported. For bare repos the LFS object store is checked at `{local_repo}/lfs/objects/`; for working-tree repos at `{local_repo}/.git/lfs/objects/`.

## How Files Are Located

Priority order in `managed_retrieve()`:

1. **Pointer resolution** (always): The file pointer at `{ref}:{path}` is resolved via the local repo (`git cat-file blob`) or the GitHub/GitLab API. For regular files the content is returned directly. For LFS files the OID is extracted from the pointer.
2. **Local LFS store** (`local_repo` setting): Looks up the LFS object by OID in the local repo's LFS object store. If found but the SHA-256 does not match the OID, a `WorkflowError` is raised.
3. **Cache** (`cache` setting): Checks the configured cache directory.
4. **Remote LFS**: Queries the LFS Batch API (`{repo_url}.git/info/lfs/objects/batch`) and downloads from the returned URL.

## License

MIT License
