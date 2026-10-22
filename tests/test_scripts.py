"""Shell scripts under scripts/: every test runs the real script with stub docker, ssh, rsync, k6 ... on PATH.

A stub logs its argv to $STUB_LOG, so each test asserts on that log. No real docker or ssh is ever called.
Set LT_SCRIPTS_DIR to another copy of scripts/ (for example a backup) to run the same tests against it.
"""

import os
import re
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(os.environ.get("LT_SCRIPTS_DIR") or ROOT / "scripts")
SYSTEM_PATH = os.environ["PATH"]

STUBS = {
    "docker": r"""#!/usr/bin/env bash
echo "docker $* (cwd=$PWD)" >> "$STUB_LOG"
d=$STUB_DIR
[ -n "${STUB_DOCKER_FAIL:-}" ] && { echo "docker: stub failure" >&2; exit 1; }
filter=
for a in "$@"; do case $a in name=*) filter=${a#name=} ;; esac; done
list() { if [ -n "$filter" ]; then grep -F -- "$filter" "$1"; else cat "$1"; fi; true; }
drop() { grep -vxF -- "$2" "$1" > "$1.n"; mv "$1.n" "$1"; }
case $1 in
  ps) list "$d/containers" ;;
  rm) a=${!#}; drop "$d/containers" "$a" ;;
  volume) case $2 in ls) cat "$d/volumes" ;; rm) drop "$d/volumes" "$3" ;; esac ;;
  network) case $2 in ls) cat "$d/networks" ;; rm) drop "$d/networks" "$3" ;; esac ;;
  image) case $2 in
    inspect) grep -qxF -- "$3" "$d/images"; exit $? ;;
    rm) drop "$d/images" "$3" ;;
    ls) cat "$d/images" ;;
  esac ;;
  images) cat "$d/dangling" ;;
  buildx) case $2 in du) cat "$d/cache" ;; esac ;;
  build) echo lt-app:1 >> "$d/images" ;;
  inspect) echo abc123 ;;
  compose)
    case " $* " in
      *" down "*) [ -n "${STUB_DOWN_FAIL:-}" ] && { echo "boom: down failed" >&2; exit 1; } ;;
      *" up "*) [ -n "${STUB_UP_FAIL:-}" ] && { echo "boom: pull access denied" >&2; exit 1; } ;;
      *" ps "*) cat "$d/ps_out" 2>/dev/null ;;
    esac ;;
esac
exit 0
""",
    "ssh": r"""#!/usr/bin/env bash
echo "ssh $*" >> "$STUB_LOG"
if [ -n "${STUB_SSH_BLOCK_ON:-}" ]; then case "$*" in *"$STUB_SSH_BLOCK_ON"*) exec /bin/sleep 30 ;; esac; fi
if [ -n "${STUB_SSH_FAIL_ON:-}" ]; then case "$*" in *"$STUB_SSH_FAIL_ON"*) echo "ssh: stub failure" >&2; exit 255 ;; esac; fi
[ -n "${STUB_SSH_EXEC:-}" ] || { case "$*" in *"test -f"*) exit 1 ;; esac; exit 0; }
while [ "${1:-}" = -o ]; do shift 2; done
shift
if [ $# -eq 1 ]; then exec bash -c "$1"; else exec "$@"; fi
""",
    "rsync": r"""#!/usr/bin/env bash
echo "rsync $*" >> "$STUB_LOG"
if [ -n "${STUB_RSYNC_ABORT_FOR:-}" ]; then case "$*" in *"$STUB_RSYNC_ABORT_FOR"*) touch "${@: -1}ABORT" ;; esac; fi
exit 0
""",
    "pkill": '#!/usr/bin/env bash\necho "pkill $*" >> "$STUB_LOG"\nexit 1\n',
    "k6": '#!/usr/bin/env bash\necho "k6 $*" >> "$STUB_LOG"\nexec /bin/sleep "${STUB_K6_SECS:-0}"\n',
    "uv": '#!/usr/bin/env bash\necho "uv $*" >> "$STUB_LOG"\nexit 0\n',
    "curl": "#!/usr/bin/env bash\nexit 0\n",
    "sleep": "#!/usr/bin/env bash\nexec /bin/sleep 0.02\n",
}


class Host:
    """A tmp sandbox: stub bin dir, HOME, stub state files and the stub log."""

    def __init__(self, tmp_path: Path):
        self.root = tmp_path
        self.home = tmp_path / "home"
        self.state = tmp_path / "state"
        self.bin = tmp_path / "bin"
        for d in (self.home, self.state, self.bin):
            d.mkdir()
        for name, body in STUBS.items():
            p = self.bin / name
            p.write_text(body)
            p.chmod(0o755)
        for f in ("containers", "volumes", "networks", "images", "dangling", "cache"):
            (self.state / f).write_text("")
        self.log = tmp_path / "stub.log"
        self.log.write_text("")

    def put(self, name: str, lines: list[str]) -> None:
        (self.state / name).write_text("".join(f"{x}\n" for x in lines))

    def read(self, name: str) -> list[str]:
        return (self.state / name).read_text().split()

    def env(self, **extra) -> dict:
        return {"PATH": f"{self.bin}:{SYSTEM_PATH}", "HOME": str(self.home), "STUB_LOG": str(self.log),
                "STUB_DIR": str(self.state), "LT_SSH": "stubhost", **extra}

    def calls(self, prefix: str = "") -> list[str]:
        """Logged stub calls that start with prefix, without the trailing '(cwd=...)'."""
        entries: list[str] = []
        for ln in self.log.read_text().splitlines():   # a multi-line argv spans several log lines
            if re.match(r"(ssh|docker|rsync|pkill|k6|uv|STUB) ", ln) or not entries:
                entries.append(ln)
            else:
                entries[-1] += "\n" + ln
        return [e.split(" (cwd=")[0] for e in entries if e.startswith(prefix)]

    def run(self, cmd: list, cwd: Path | None = None, timeout: int = 60, **env) -> subprocess.CompletedProcess:
        return subprocess.run([str(c) for c in cmd], capture_output=True, text=True, timeout=timeout,
                              cwd=cwd or self.root, env=self.env(**env), stdin=subprocess.DEVNULL)

    def tree(self, *names: str) -> Path:
        """A copy of the named scripts (plus helper libs, sql stubs) under <tmp>/work, so a run writes nothing into the repo."""
        work = self.root / "work"
        (work / "scripts" / "remote").mkdir(parents=True, exist_ok=True)
        (work / "results").mkdir(exist_ok=True)
        (work / "sql").mkdir(exist_ok=True)
        for n in ("reset_stats", "pgstats", "pgstats_kv", "invariants", "facts"):
            (work / "sql" / f"{n}.sql").write_text("")
        for n in (*names, "pg_guard_lib.sh"):
            src = SCRIPTS / n
            if src.exists():
                shutil.copy(src, work / "scripts" / n)
        return work

    def stub_script(self, work: Path, name: str, code: int = 0, stderr: str = "") -> None:
        p = work / "scripts" / name
        p.write_text(f'#!/usr/bin/env bash\necho "STUB {name} $*" >> "$STUB_LOG"\n'
                     + (f'echo "{stderr}" >&2\n' if stderr else "") + f"exit {code}\n")
        p.chmod(0o755)


@pytest.fixture
def host(tmp_path):
    return Host(tmp_path)


def run_signalled(host: Host, cmd: list, cwd: Path, marker: str, **env) -> subprocess.CompletedProcess:
    """Start cmd in its own process group, wait until the stub log has marker, send SIGTERM to the group."""
    p = subprocess.Popen([str(c) for c in cmd], cwd=cwd, env=host.env(**env), start_new_session=True,
                         stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    deadline = time.time() + 20
    while marker not in host.log.read_text() and time.time() < deadline and p.poll() is None:
        time.sleep(0.05)
    assert marker in host.log.read_text(), f"never reached {marker!r}"
    os.killpg(p.pid, signal.SIGTERM)
    try:
        out, err = p.communicate(timeout=20)
    finally:
        try:
            os.killpg(p.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    return subprocess.CompletedProcess(p.args, p.returncode, out, err)


# ---------------------------------------------------------------- cleanup.sh (A1)

CLEANUP = SCRIPTS / "remote" / "cleanup.sh"
START = "2026-10-02 14:55"


def cleanup(host: Host, *args, **env):
    return host.run(["bash", CLEANUP, *args], **env)


def removed(host: Host, kind: str) -> list[str]:
    """Names passed to docker rm / volume rm / network rm."""
    pre = {"container": "docker rm ", "volume": "docker volume rm ", "network": "docker network rm "}[kind]
    return [ln.split()[-1] for ln in host.calls(pre)]


def test_cleanup_name_contains_lt_but_not_prefix_apply_keeps_it(host):
    host.put("containers", ["lt-pg", "vault-lt-1", "default-lt-x", "lt-redis"])
    host.put("volumes", ["lt-pgdata", "vault-lt-1", "default-lt-x"])
    host.put("networks", ["lt-net", "default-lt-x"])
    r = cleanup(host, START, "--apply")
    assert r.returncode == 0, r.stderr
    assert sorted(removed(host, "container")) == ["lt-pg", "lt-redis"]
    assert removed(host, "volume") == ["lt-pgdata"]
    assert removed(host, "network") == ["lt-net"]
    assert host.read("containers") == ["vault-lt-1", "default-lt-x"]
    assert "containers: 0" in r.stdout


def test_cleanup_dry_run_lists_but_removes_nothing(host):
    host.put("containers", ["lt-pg"])
    host.put("images", ["lt-app:1"])
    (host.home / ".lt-preexisting-images").write_text("alpine:3\n")
    r = cleanup(host, START)
    assert r.returncode == 0, r.stderr
    assert "+ docker rm -f -v lt-pg" in r.stdout
    assert "+ docker image rm lt-app:1" in r.stdout
    assert removed(host, "container") == [] and host.calls("docker image rm") == []
    assert host.calls("pkill") == []


def test_cleanup_images_removes_only_those_absent_from_the_inventory(host):
    host.put("images", ["lt-app:1", "postgres:18.6", "redis:7.2-alpine", "unrelated:1"])
    (host.home / ".lt-preexisting-images").write_text("postgres:18.6\nunrelated:1\n")
    r = cleanup(host, START, "--apply")
    assert r.returncode == 0, r.stderr
    assert sorted(ln.split()[-1] for ln in host.calls("docker image rm")) == ["lt-app:1", "redis:7.2-alpine"]
    assert sorted(host.read("images")) == ["postgres:18.6", "unrelated:1"]
    assert not (host.home / ".lt-preexisting-images").exists()  # the next deploy records a fresh inventory


def test_cleanup_missing_inventory_removes_no_image_and_says_why(host):
    host.put("images", ["lt-app:1", "postgres:18.6"])
    r = cleanup(host, START, "--apply")
    assert r.returncode == 0, r.stderr
    assert host.calls("docker image rm") == []
    assert "no image is removed" in r.stdout and ".lt-preexisting-images is missing" in r.stdout


def test_cleanup_empty_inventory_file_removes_every_listed_image(host):
    host.put("images", ["lt-app:1", "postgres:18.6"])
    (host.home / ".lt-preexisting-images").write_text("")
    r = cleanup(host, START, "--apply")
    assert sorted(ln.split()[-1] for ln in host.calls("docker image rm")) == ["lt-app:1", "postgres:18.6"]


@pytest.mark.parametrize("bad", ["", "   ", "\t\n", "garbage", "2026-13-45 99:99", "14:55", "2026-10-02"])
def test_cleanup_unparseable_start_exits_nonzero_and_touches_nothing(host, bad):
    host.put("containers", ["lt-pg"])
    r = cleanup(host, bad, "--apply")
    assert r.returncode != 0
    assert "START" in r.stderr
    assert host.calls("docker") == [] and host.calls("pkill") == []


def test_cleanup_missing_start_argument_exits_nonzero(host):
    r = cleanup(host)
    assert r.returncode != 0 and "START" in r.stderr
    assert host.calls("docker") == []


def test_cleanup_unknown_flag_exits_nonzero_and_touches_nothing(host):
    host.put("containers", ["lt-pg"])
    r = cleanup(host, START, "--aply")
    assert r.returncode == 2
    assert host.calls("docker") == []


@pytest.mark.parametrize("start", [START, "2026-10-02 14:55:00", "2026-10-02T14:55:00Z", "2026-10-02T14:55:00+00:00"])
def test_cleanup_start_formats_are_accepted(host, start):
    assert cleanup(host, start).returncode == 0


def test_cleanup_build_cache_compares_datetimes_not_string_prefixes(host):
    rows = [
        ("new_rfc", "2026-10-02T15:00:00.123456789Z"),
        ("old_rfc", "2026-10-02T14:54:59Z"),
        ("new_plus8", "2026-10-02T23:30:00+08:00"),     # 15:30Z: after START
        ("old_plus8", "2026-10-02T22:00:00+08:00"),     # 14:00Z: before START although "22" > "14"
        ("new_docker", "2026-10-02 15:01:00 +0000 UTC"),
        ("old_docker", "2026-10-02 14:00:00 +0000 UTC"),
        ("edge_same", "2026-10-02T14:55:00Z"),
    ]
    host.put("cache", [f'{{"ID":"{i}","CreatedAt":"{t}","Description":"x"}}' for i, t in rows]
             + ['{"ID":"broken","CreatedAt":"yesterday"}', '{"ID":"nodate"}', ""])
    r = cleanup(host, START, "--apply")
    assert r.returncode == 0, r.stderr
    pruned = [ln.split("id=")[-1] for ln in host.calls("docker buildx prune")]
    assert sorted(pruned) == ["edge_same", "new_docker", "new_plus8", "new_rfc"]
    assert "unreadable CreatedAt" in r.stderr
    assert "build-cache entries after start: 4" in r.stdout


def test_cleanup_dangling_images_use_the_same_datetime_compare(host):
    host.put("dangling", ['{"ID":"d_new","CreatedAt":"2026-10-02 15:00:00 +0000 UTC","Repository":"<none>"}',
                          '{"ID":"d_old","CreatedAt":"2026-10-01 15:00:00 +0000 UTC","Repository":"<none>"}'])
    r = cleanup(host, START, "--apply")
    assert [ln.split()[-1] for ln in host.calls("docker image rm")] == ["d_new"]


def test_cleanup_honours_lt_remote_dir_and_leaves_the_default_dir_alone(host):
    for name in ("lt-loadtest", "other-loadtest"):
        (host.home / name).mkdir()
        (host.home / name / "keep.txt").write_text("x")
    r = cleanup(host, START, "--apply", LT_REMOTE_DIR="other-loadtest")
    assert r.returncode == 0, r.stderr
    assert not (host.home / "other-loadtest").exists()
    assert (host.home / "lt-loadtest" / "keep.txt").exists()
    down = [ln for ln in host.log.read_text().splitlines() if " down -v" in ln]
    assert len(down) == 1 and down[0].endswith("(cwd=" + str(host.home / "other-loadtest") + ")")


def test_cleanup_default_dir_is_lt_loadtest(host):
    (host.home / "lt-loadtest").mkdir()
    assert cleanup(host, START, "--apply").returncode == 0
    assert not (host.home / "lt-loadtest").exists()


def test_cleanup_absolute_lt_remote_dir_is_used_as_is(host, tmp_path):
    target = tmp_path / "elsewhere"
    target.mkdir()
    assert cleanup(host, START, "--apply", LT_REMOTE_DIR=str(target)).returncode == 0
    assert not target.exists()


@pytest.mark.parametrize("bad", [".", "..", "/", "./", "x/.."])
def test_cleanup_lt_remote_dir_that_is_home_or_its_parent_is_refused(host, bad):
    (host.home / "precious").write_text("x")
    r = cleanup(host, START, "--apply", LT_REMOTE_DIR=bad)
    assert r.returncode == 2 and "refusing" in r.stderr
    assert (host.home / "precious").exists()
    assert host.calls("docker") == []


def test_cleanup_stops_orphan_guard_and_sampler_anchored_to_the_remote_dir(host):
    import re
    r = cleanup(host, START, "--apply", LT_REMOTE_DIR="other-loadtest")
    pk = host.calls("pkill")
    assert len(pk) == 1, r.stdout
    pat = pk[0].removeprefix("pkill -f ")
    d = host.home / "other-loadtest"
    assert re.search(pat, f"bash {d}/scripts/remote/guard.sh {d}/results/t1")
    assert re.search(pat, f"bash {d}/scripts/remote/sampler.sh {d}/results/t1/s.csv 5")
    assert not re.search(pat, f"bash {host.home}/lt-loadtest/scripts/remote/guard.sh /x")
    assert not re.search(pat, f"bash {d}-other/scripts/remote/guard.sh /x")
    assert not re.search(pat, "bash -s -- 2026-10-02 --apply")


def test_cleanup_docker_failure_exits_nonzero_without_a_clean_proof(host):
    r = cleanup(host, START, "--apply", STUB_DOCKER_FAIL="1")
    assert r.returncode != 0
    assert "docker" in r.stderr
    assert "containers: 0" not in r.stdout
    assert host.calls("pkill") == []


def test_cleanup_fires_twice_second_run_succeeds_and_removes_nothing_more(host):
    (host.home / "lt-loadtest").mkdir()
    host.put("containers", ["lt-pg", "vault-lt-1"])
    host.put("images", ["lt-app:1"])
    (host.home / ".lt-preexisting-images").write_text("alpine:3\n")
    assert cleanup(host, START, "--apply").returncode == 0
    first = len(host.calls("docker rm "))
    r2 = cleanup(host, START, "--apply")
    assert r2.returncode == 0, r2.stderr
    assert len(host.calls("docker rm ")) == first == 1
    assert len(host.calls("docker image rm")) == 1
    assert host.read("containers") == ["vault-lt-1"]


# ---------------------------------------------------------------- cpu.sh, sampler.sh (A1)

def test_cpu_lists_only_containers_whose_name_starts_with_lt(host):
    host.put("containers", ["lt-pg", "vault-lt-1", "default-lt-x"])
    r = host.run(["bash", SCRIPTS / "remote" / "cpu.sh"])
    assert [ln.split()[0] for ln in r.stdout.splitlines()] == ["lt-pg"]
    assert [ln for ln in host.calls("docker inspect")] == ["docker inspect -f {{.Id}} lt-pg"]


def test_cpu_no_matching_container_prints_nothing(host):
    host.put("containers", ["vault-lt-1"])
    r = host.run(["bash", SCRIPTS / "remote" / "cpu.sh"])
    assert r.returncode == 0 and r.stdout == ""


def test_sampler_inspects_only_containers_whose_name_starts_with_lt(host):
    host.put("containers", ["lt-pg", "vault-lt-1", "default-lt-x"])
    out = host.root / "s.csv"
    subprocess.run(["timeout", "1", "bash", str(SCRIPTS / "remote" / "sampler.sh"), str(out), "0.2"],
                   env=host.env(), capture_output=True, timeout=10, stdin=subprocess.DEVNULL)
    inspected = set(host.calls("docker inspect"))
    assert inspected == {"docker inspect -f {{.Id}} lt-pg"}
    assert out.read_text().startswith("ts,container,cpu_usec,mem_bytes")


# ---------------------------------------------------------------- deploy.sh (A1 inventory)

def deploy(host: Host, **env):
    remote = host.root / "remote"
    remote.mkdir(exist_ok=True)
    return host.run(["bash", SCRIPTS / "deploy.sh"], LT_REMOTE_DIR=str(remote), STUB_SSH_EXEC="1", **env)


def test_deploy_records_pre_test_images_before_the_build(host):
    host.put("images", ["alpine:3", "postgres:18.6"])
    r = deploy(host)
    assert r.returncode == 0, r.stderr
    inv = (host.home / ".lt-preexisting-images").read_text().split()
    assert inv == ["alpine:3", "postgres:18.6"]
    assert "lt-app:1" not in inv and "lt-app:1" in host.read("images")
    assert not (host.home / ".lt-preexisting-images.tmp").exists()


def test_deploy_twice_does_not_record_the_tests_own_images(host):
    host.put("images", ["alpine:3"])
    assert deploy(host).returncode == 0
    assert "lt-app:1" in host.read("images")
    assert deploy(host).returncode == 0
    assert (host.home / ".lt-preexisting-images").read_text().split() == ["alpine:3"]


def test_deploy_docker_failure_stops_before_the_build_and_leaves_no_inventory(host):
    r = deploy(host, STUB_DOCKER_FAIL="1")
    assert r.returncode != 0
    assert not (host.home / ".lt-preexisting-images").exists()
    assert host.calls("docker build") == []


def test_deploy_empty_host_records_an_empty_inventory(host):
    assert deploy(host).returncode == 0
    assert (host.home / ".lt-preexisting-images").read_text() == ""


# ---------------------------------------------------------------- restack.sh (A14)

HEALTHY = ["lt-pg running healthy", "lt-redis running healthy", "lt-collector running "]


def restack(host: Host, *args, **env):
    remote = host.root / "remote"
    remote.mkdir(exist_ok=True)
    return host.run(["bash", SCRIPTS / "restack.sh", *args], LT_REMOTE_DIR=str(remote), STUB_SSH_EXEC="1", **env)


def test_restack_all_services_healthy_exits_zero_and_lists_them(host):
    host.put("ps_out", HEALTHY)
    r = restack(host)
    assert r.returncode == 0, r.stderr
    assert "lt-pg" in r.stdout


def test_restack_up_failure_exits_nonzero_shows_compose_stderr_and_skips_the_wait(host):
    host.put("ps_out", HEALTHY)
    r = restack(host, STUB_UP_FAIL="1")
    assert r.returncode != 0
    assert "pull access denied" in r.stderr
    assert not any(" ps " in c for c in host.calls("docker compose"))


def test_restack_down_failure_exits_nonzero_and_does_not_start_the_stack(host):
    host.put("ps_out", HEALTHY)
    r = restack(host, STUB_DOWN_FAIL="1")
    assert r.returncode != 0 and "down failed" in r.stderr
    assert not any(" up " in c for c in host.calls("docker compose"))


@pytest.mark.parametrize("ps", [
    ["lt-pg running unhealthy", "lt-redis running healthy"],
    ["lt-pg running starting", "lt-redis running healthy"],
    ["lt-pg running healthy", "lt-collector exited "],
    ["lt-pg running healthy", "lt-collector restarting "],
    [],
    ["", "  "],
])
def test_restack_service_not_running_and_healthy_exits_one_after_the_deadline(host, ps):
    host.put("ps_out", ps)
    r = restack(host)
    assert r.returncode == 1, r.stdout
    assert "not running and healthy" in r.stderr


def test_restack_passes_env_overrides_and_compose_files_to_compose(host):
    host.put("ps_out", HEALTHY)
    r = restack(host, "PG_INITDB_ARGS=", "--", "-f", "compose.yaml")
    assert r.returncode == 0, r.stderr
    assert any("-f compose.yaml up -d" in c for c in host.calls("docker compose"))


# ---------------------------------------------------------------- batch*.sh (A14)

BATCH_STUBS = ["restack.sh", "run_test.sh", "pg_bench_matrix.sh", "ab_runs.sh", "pg_ablation.sh", "build_rust.sh",
               "t3.sh", "t6.sh", "t6b.sh", "t7.sh", "pg_fk_check.sh"]
BATCHES = [f"batch{n}.sh" for n in range(2, 9)]


def batch_tree(host: Host, name: str, deploy_code: int):
    work = host.tree(name)
    for s in BATCH_STUBS:
        host.stub_script(work, s)
    host.stub_script(work, "deploy.sh", code=deploy_code, stderr="deploy: docker build failed")
    for d in ("results/private", "results/t2_soak", "results/t1_baseline", "results/t4_rootcause"):
        (work / d).mkdir(parents=True, exist_ok=True)
    return work


@pytest.mark.parametrize("name", BATCHES)
def test_batch_failed_deploy_stops_the_batch_and_shows_the_error(host, name):
    work = batch_tree(host, name, deploy_code=1)
    r = host.run(["bash", work / "scripts" / name], cwd=work, LT_REMOTE_DIR="lt-loadtest")
    assert r.returncode != 0
    assert "deployed" not in r.stdout
    assert "deploy: docker build failed" in r.stderr
    assert [c for c in host.calls("STUB ") if "deploy.sh" not in c] == []   # no later step ran


@pytest.mark.parametrize("name", [f"batch{n}.sh" for n in range(3, 9)])
def test_batch_failed_restack_stops_the_batch(host, name):
    work = batch_tree(host, name, deploy_code=0)
    host.stub_script(work, "restack.sh", code=1, stderr="restack: lt-pg not healthy")
    r = host.run(["bash", work / "scripts" / name], cwd=work, LT_REMOTE_DIR="lt-loadtest")
    assert r.returncode != 0
    assert "restack failed" in r.stderr
    steps = [c.split()[1] for c in host.calls("STUB ")]
    assert steps[-1] == "restack.sh"           # nothing ran on the stack that failed to come up
    assert steps.count("restack.sh") == 1


def test_batch_successful_deploy_runs_the_steps(host):
    work = batch_tree(host, "batch2.sh", deploy_code=0)
    r = host.run(["bash", work / "scripts" / "batch2.sh"], cwd=work)
    assert r.returncode == 0, r.stderr
    assert "deployed" in r.stdout
    assert [c.split()[1] for c in host.calls("STUB ")] == ["deploy.sh", "pg_bench_matrix.sh", "ab_runs.sh"]


# ---------------------------------------------------------------- run_test.sh, t6.sh, t6b.sh (A14: trap)

def stop_lines(host: Host, pidname: str) -> list[str]:
    return [ln for ln in host.calls("ssh") if "bg.sh stop" in ln and pidname in ln]


def test_run_test_normal_end_stops_guard_and_sampler_exactly_once(host):
    work = host.tree("run_test.sh")
    r = host.run(["bash", work / "scripts" / "run_test.sh", "tx"], cwd=work)
    assert r.returncode == 0, r.stderr
    assert len(stop_lines(host, "guard.pid")) == 1
    assert "sampler.pid" in stop_lines(host, "guard.pid")[0]


def test_run_test_sigterm_during_k6_stops_guard_and_sampler(host):
    work = host.tree("run_test.sh")
    r = run_signalled(host, ["bash", work / "scripts" / "run_test.sh", "tx"], work, "k6 run", STUB_K6_SECS="30")
    assert r.returncode != 0
    lines = stop_lines(host, "guard.pid")
    assert len(lines) == 1 and "sampler.pid" in lines[0]


def test_run_test_guard_start_failure_refuses_and_registers_no_stop(host):
    work = host.tree("run_test.sh")
    r = host.run(["bash", work / "scripts" / "run_test.sh", "tx"], cwd=work, STUB_SSH_FAIL_ON="bg.sh start")
    assert r.returncode == 1 and "refusing to run" in r.stderr
    assert host.calls("k6") == []
    assert stop_lines(host, "guard.pid") == []


def test_t6_sigterm_while_a_store_runs_stops_the_guard(host):
    work = host.tree("t6.sh")
    r = run_signalled(host, ["bash", work / "scripts" / "t6.sh"], work, "--name lt-cache-", STUB_SSH_BLOCK_ON="--name lt-cache-")
    assert r.returncode != 0
    assert len(stop_lines(host, "guard.pid")) == 1


def test_t6_normal_end_stops_the_guard_exactly_once(host):
    work = host.tree("t6.sh")
    r = host.run(["bash", work / "scripts" / "t6.sh"], cwd=work)
    assert r.returncode == 0, r.stderr
    assert len(stop_lines(host, "guard.pid")) == 1


def test_t6b_sigterm_during_a_bench_stops_the_guard(host):
    work = host.tree("t6b.sh")
    r = run_signalled(host, ["bash", work / "scripts" / "t6b.sh"], work, "bench.pg_bench", STUB_SSH_BLOCK_ON="bench.pg_bench")
    assert r.returncode != 0
    assert len(stop_lines(host, "guard.pid")) == 1


def test_t6b_normal_end_stops_the_guard_exactly_once(host):
    work = host.tree("t6b.sh")
    r = host.run(["bash", work / "scripts" / "t6b.sh"], cwd=work)
    assert r.returncode == 0, r.stderr
    assert len(stop_lines(host, "guard.pid")) == 1


# ---------------------------------------------------------------- pg_*.sh (A14: guard + ABORT skip)

PG_SCRIPTS = {"pg_fk_check.sh": ("pgb_nofk_r1", 2), "pg_bench_matrix.sh": ("pgb_a_default", 4),
              "pg_ablation.sh": ("pgb_a_default_r2", 8), "pg_plan_check.sh": ("pg_plan_av_default", 2)}


def pg_tree(host: Host, name: str) -> Path:
    work = host.tree(name)
    host.stub_script(work, "restack.sh")
    return work


def pgbench_idx(host: Host) -> list[int]:
    return [i for i, ln in enumerate(host.calls("ssh")) if "pgbench -h" in ln]


@pytest.mark.parametrize("name,spec", PG_SCRIPTS.items())
def test_pg_script_starts_the_guard_before_each_pgbench_and_stops_it_after(host, name, spec):
    work = pg_tree(host, name)
    r = host.run(["bash", work / "scripts" / name], cwd=work)
    assert r.returncode == 0, r.stderr
    ssh = host.calls("ssh")
    starts = [i for i, ln in enumerate(ssh) if "bg.sh start" in ln and "guard.sh" in ln]
    stops = [i for i, ln in enumerate(ssh) if "bg.sh stop" in ln and "guard.pid" in ln]
    bench = pgbench_idx(host)
    assert len(starts) == len(stops) == len(bench) == spec[1]
    for s, b, e in zip(starts, bench, stops):
        assert s < b < e


@pytest.mark.parametrize("name,spec", PG_SCRIPTS.items())
def test_pg_script_guard_start_failure_skips_the_run_and_exits_nonzero(host, name, spec):
    work = pg_tree(host, name)
    r = host.run(["bash", work / "scripts" / name], cwd=work, STUB_SSH_FAIL_ON="bg.sh start")
    assert r.returncode != 0
    assert "guard did not start" in r.stderr
    assert pgbench_idx(host) == []
    assert len(host.calls("STUB restack")) == 1   # the first run never started, nothing after it either


@pytest.mark.parametrize("name,spec", PG_SCRIPTS.items())
def test_pg_script_abort_in_a_run_skips_every_remaining_run(host, name, spec):
    work = pg_tree(host, name)
    first, total = spec
    r = host.run(["bash", work / "scripts" / name], cwd=work, STUB_RSYNC_ABORT_FOR=f"results/{first}/")
    assert r.returncode != 0
    assert len(pgbench_idx(host)) == 1
    if total > 1:
        assert "red line hit earlier" in r.stderr


@pytest.mark.parametrize("name", ["pg_fk_check.sh", "pg_bench_matrix.sh"])
def test_pg_script_sigterm_during_pgbench_stops_the_guard(host, name):
    work = pg_tree(host, name)
    r = run_signalled(host, ["bash", work / "scripts" / name], work, "pgbench -h", STUB_SSH_BLOCK_ON="pgbench -h")
    assert r.returncode != 0
    stops = [ln for ln in host.calls("ssh") if "bg.sh stop" in ln and "guard.pid" in ln]
    assert len(stops) == 1
