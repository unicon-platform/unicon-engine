import logging
from pathlib import Path

import requests
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

logger = logging.getLogger("unicon_runner")


def download_file(url: str, path: Path, overwrite: bool = False, chunk_size: int = 8192) -> bool:
    if path.exists() and not overwrite:
        logger.info(f"File {path} already exists, skipping download")
        return True
    try:
        resp = requests.get(url, stream=True)
        resp.raise_for_status()

        file_size = int(resp.headers.get("Content-Length", 0))  # in bytes
        with (
            path.open("wb") as out_file,
            Progress(
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                expand=True,
            ) as progress,
        ):
            logger.info(f"Downloading file from {url} to {path}")
            dload_task = progress.add_task("", total=file_size)
            for chunk in resp.iter_content(chunk_size=chunk_size):
                progress.update(dload_task, advance=out_file.write(chunk))
        return True
    except requests.exceptions.RequestException as request_error:
        logger.error(f"Failed to download file from {url}: {request_error}")
        return False
