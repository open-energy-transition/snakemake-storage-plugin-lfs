# SPDX-FileCopyrightText: Contributors to PyPSA-Eur <https://github.com/pypsa/pypsa-eur>
#
# SPDX-License-Identifier: MIT

import os
import shutil
from pathlib import Path

from snakemake_interface_common.logging import get_logger

logger = get_logger()


def link_or_copy(src: Path, dst: Path, may_symlink: bool = True) -> None:
    src = src.resolve()
    funcs = [os.link, os.symlink] if may_symlink else [os.link]
    for func in funcs:
        try:
            func(src, dst)
            logger.debug(f"{func.__name__}: {src} -> {dst}")
            return
        except OSError:
            continue
    shutil.copy2(src, dst)
    logger.debug(f"copy2: {src} -> {dst}")
