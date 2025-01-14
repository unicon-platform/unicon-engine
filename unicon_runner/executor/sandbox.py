import logging
import stat
from pathlib import Path

from unicon_runner.constants import CONTY_DOWNLOAD_URL, CONTY_PATH
from unicon_runner.executor.unsafe import UnsafeExecutor
from unicon_runner.helpers import download_file

logger = logging.getLogger("unicon_runner")


class SandboxExecutor(UnsafeExecutor):
    def __init__(self, root_dir: Path):
        if not (conty_bin := Path(CONTY_PATH)).exists():
            logger.info("`conty` binary not found, downloading...")
            if download_file(CONTY_DOWNLOAD_URL, conty_bin, overwrite=True) is False:
                raise RuntimeError(f"Failed to download `conty` binary from {CONTY_DOWNLOAD_URL}")
            # `chmod +x` the downloaded binary
            conty_bin.chmod(conty_bin.stat().st_mode | stat.S_IEXEC)

        super().__init__(root_dir)

    def _cmd(self, cwd: Path) -> tuple[list[str], dict[str, str]]:
        assert CONTY_PATH is not None

        # NOTE: `uv` binary is assumed to be stored under `~/.local/bin/`
        # We are using `uv` as the environment manager and program runner
        uv_path = Path("~/.local/bin/uv").expanduser()
        # NOTE: We need to bind the uv app state folder to access uv-managed python executables
        uv_app_state_path = Path("~/.local/share/uv").expanduser()
        uv_cache_path = Path("~/.cache/uv").expanduser()

        # fmt: off
        return [
            CONTY_PATH,
            "--ro-bind", *(["/"] * 2),
            "--ro-bind", *([str(uv_path)] * 2),
            "--ro-bind", *([str(uv_app_state_path)] * 2),
            "--bind", *([str(uv_cache_path)] * 2),
            # R/W bind to the root working directory
            "--bind", *([str(cwd.parents[0])] * 2),
            # NOTE: Mount `procfs` to allow access to process information
            # This seems be required for GPU workloads
            "--proc", "/proc",
            # Bind /dev to allow access to devices
            "--dev-bind", *(["/dev"] * 2),
            str(cwd / self.ENTRYPOINT),
        ], {
            # Conty specific environment variables
            "SANDBOX": "1", "SANDBOX_LEVEL": "1", "QUIET_MODE": "1",
            # NOTE: We need to unset VIRTUAL_ENV to prevent uv from using the wrong base python interpreter
            "VIRTUAL_ENV": "''",
        }
        # fmt: off
