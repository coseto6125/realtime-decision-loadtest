"""scripts/remote/bg.sh: the background helpers must never run a bare `kill` on an empty PID."""

import subprocess
import time
from pathlib import Path

BG = str(Path(__file__).resolve().parents[1] / "scripts" / "remote" / "bg.sh")


def run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["bash", BG, *args], capture_output=True, text=True, timeout=10)


def test_bg_stop_missing_pidfile_fails_loudly(tmp_path):
    r = run("stop", str(tmp_path / "nope.pid"))
    assert r.returncode == 1
    assert "no PID" in r.stderr
    assert "usage: kill" not in r.stderr


def test_bg_stop_empty_pidfile_fails_loudly(tmp_path):
    pidfile = tmp_path / "empty.pid"
    pidfile.write_text("")
    r = run("stop", str(pidfile))
    assert r.returncode == 1
    assert "no PID" in r.stderr


def test_bg_stop_whitespace_pid_fails_loudly(tmp_path):
    pidfile = tmp_path / "ws.pid"
    pidfile.write_text("  \n")
    r = run("stop", str(pidfile))
    assert r.returncode == 1
    assert "bad PID" in r.stderr


def test_bg_start_creates_missing_dir_and_stop_kills(tmp_path):
    pidfile = tmp_path / "sub" / "dir" / "sleep.pid"
    assert run("start", str(pidfile), "sleep", "30").returncode == 0
    pid = int(pidfile.read_text())
    assert run("alive", str(pidfile)).returncode == 0
    assert run("stop", str(pidfile)).returncode == 0
    time.sleep(0.2)
    assert subprocess.run(["kill", "-0", str(pid)], capture_output=True).returncode != 0
    assert not pidfile.exists()


def test_bg_start_command_that_exits_at_once_fails(tmp_path):
    r = run("start", str(tmp_path / "x.pid"), "false")
    assert r.returncode == 1
    assert "exited right after start" in r.stderr


def test_bg_stop_dead_pid_fails_loudly(tmp_path):
    pidfile = tmp_path / "dead.pid"
    p = subprocess.Popen(["true"])
    p.wait()
    pidfile.write_text(str(p.pid))
    r = run("stop", str(pidfile))
    assert r.returncode == 1
    assert "not running" in r.stderr
