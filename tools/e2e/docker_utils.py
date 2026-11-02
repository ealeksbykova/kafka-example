"""Minimal docker helpers for the E2E harness.

Only the standard library is used, so the harness runs on a plain Python installation
(no docker SDK, no requests) - `docker compose` itself does the real work.
"""

from __future__ import annotations

import subprocess
import time
from typing import Iterable, Sequence


class ComposeError(RuntimeError):
    """Raised when a docker compose command fails."""


def run(
    args: Sequence[str],
    *,
    cwd: str,
    timeout: int = 600,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    """Runs a command and returns the completed process.

    Output is captured so failures can be reported verbatim in the test output.
    """
    completed = subprocess.run(
        list(args),
        cwd=cwd,
        timeout=timeout,
        check=False,
        capture_output=capture,
        text=True,
    )
    if check and completed.returncode != 0:
        raise ComposeError(
            "command failed: {cmd}\nstdout:\n{out}\nstderr:\n{err}".format(
                cmd=" ".join(args),
                out=(completed.stdout or "").strip(),
                err=(completed.stderr or "").strip(),
            )
        )
    return completed


def compose(compose_file: str, *args: str, cwd: str, timeout: int = 600, check: bool = True):
    """Invokes `docker compose -f <file> ...`."""
    return run(
        ["docker", "compose", "-f", compose_file, *args],
        cwd=cwd,
        timeout=timeout,
        check=check,
    )


def docker_available() -> bool:
    """True when a usable Docker daemon is reachable."""
    try:
        result = run(["docker", "info", "--format", "{{.ServerVersion}}"], cwd=".", timeout=30, check=False)
    except (OSError, subprocess.SubprocessError):
        return False
    return result.returncode == 0


def container_exists(name: str) -> bool:
    """True when a container with exactly this name exists, running or not."""
    result = run(
        ["docker", "ps", "-a", "--filter", "name=^/{0}$".format(name), "--format", "{{.Names}}"],
        cwd=".",
        timeout=60,
        check=False,
    )
    return name in (result.stdout or "").split()


def container_running(name: str) -> bool:
    """True when the named container is running."""
    result = run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        cwd=".",
        timeout=60,
        check=False,
    )
    return (result.stdout or "").strip() == "true"


def container_health(name: str) -> str:
    """Returns the health status of a container ('healthy', 'unhealthy', 'starting' or '')."""
    result = run(
        ["docker", "inspect", "-f", "{{if .State.Health}}{{.State.Health.Status}}{{end}}", name],
        cwd=".",
        timeout=60,
        check=False,
    )
    return (result.stdout or "").strip()


def wait_until(predicate, *, timeout: float, interval: float = 1.0, description: str = "condition"):
    """Polls `predicate` until it returns a truthy value or the timeout expires."""
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except Exception as exc:  # noqa: BLE001 - polling helper, any error means "not ready yet"
            last_error = exc
        time.sleep(interval)
    raise TimeoutError("timed out after {0}s waiting for {1}".format(timeout, description)) from last_error


def wait_for_containers(names: Iterable[str], *, timeout: float = 180.0) -> None:
    """Waits until every listed container reports `healthy`."""

    def all_healthy() -> bool:
        return all(container_health(name) == "healthy" for name in names)

    wait_until(all_healthy, timeout=timeout, description="containers to become healthy: " + ", ".join(names))


def container_logs(name: str, *, tail: int = 200) -> str:
    """Returns the last lines of a container's log, used for failure diagnostics."""
    result = run(["docker", "logs", "--tail", str(tail), name], cwd=".", timeout=120, check=False)
    return ((result.stdout or "") + (result.stderr or "")).strip()

