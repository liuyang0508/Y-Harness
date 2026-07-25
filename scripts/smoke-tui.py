#!/usr/bin/env python3
"""Exercise yh-tui against yh serve-demo through a real POSIX pseudo-terminal."""

import argparse
import errno
import fcntl
import os
import pty
import select
import sqlite3
import struct
import subprocess
import tempfile
import termios
import time


def read_until(master: int, process: subprocess.Popen[bytes], output: bytearray,
               needle: bytes, deadline: float) -> None:
    while needle not in output:
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for {needle!r}")
        readable, _, _ = select.select([master], [], [], 0.1)
        if readable:
            try:
                chunk = os.read(master, 65536)
            except OSError as error:
                if error.errno == errno.EIO and process.poll() is not None:
                    break
                raise
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > 4 * 1024 * 1024:
                raise RuntimeError("TUI emitted more than 4 MiB during smoke test")
        elif process.poll() is not None:
            break
    if needle not in output:
        raise RuntimeError(
            f"TUI exited before emitting {needle!r}; status={process.poll()}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tui", default="target/debug/yh-tui")
    parser.add_argument("--engine", default="target/debug/yh")
    parser.add_argument(
        "--configured",
        action="store_true",
        help="initialize and connect to a persistent Engine project",
    )
    options = parser.parse_args()
    tui = os.path.abspath(options.tui)
    engine = os.path.abspath(options.engine)

    with tempfile.TemporaryDirectory(prefix="y-harness-tui-") as working_directory:
        if options.configured:
            project = os.path.join(working_directory, "project")
            subprocess.run(
                [engine, "init", project],
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            command = [
                tui,
                "--config",
                os.path.join(project, "y-harness.json"),
                "--engine",
                engine,
            ]
            database = os.path.join(project, ".y-harness", "state.db")
        else:
            command = [tui, "--demo", "--engine", engine]
            database = os.path.join(working_directory, ".y-harness", "state.db")
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", 32, 120, 0, 0))
        process = subprocess.Popen(
            command,
            cwd=working_directory,
            stdin=slave,
            stdout=slave,
            stderr=slave,
            close_fds=True,
        )
        os.close(slave)
        output = bytearray()
        deadline = time.monotonic() + 15
        try:
            read_until(master, process, output, b"READY", deadline)
            os.write(master, b"TUI PTY smoke\r")
            read_until(master, process, output, b"observed tool output", deadline)
            os.write(master, b"/quit\r")
            read_until(master, process, output, b"\x1b[?1049l", deadline)
            process.wait(timeout=max(0.1, deadline - time.monotonic()))
        finally:
            os.close(master)
            if process.poll() is None:
                process.kill()
                process.wait()

        if process.returncode != 0:
            raise RuntimeError(f"TUI exited with status {process.returncode}")
        for sequence in (b"\x1b[?1049h", b"\x1b[?1049l", b"\x1b[?2004h", b"\x1b[?2004l"):
            if sequence not in output:
                raise RuntimeError(f"terminal lifecycle sequence missing: {sequence!r}")

        with sqlite3.connect(database) as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM events "
                "WHERE event_json LIKE '%\"type\":\"user_message\"%' "
                "AND event_json LIKE '%TUI PTY smoke%'"
            ).fetchone()
        if row is None or row[0] != 1:
            raise RuntimeError("authoritative State did not contain the submitted prompt")

    mode = "configured" if options.configured else "demo"
    print(f"yh-tui PTY smoke ({mode}): ok")


if __name__ == "__main__":
    main()
