"""Run one vendor benchmark in an isolated child process group with hard limits.

Why: the in-process SIGALRM timeout in bench_common.run_timed() cannot interrupt
a thread that is blocked inside a C extension (database client libraries), so a
single hung vendor could stall a whole suite for hours. Here the parent never
runs vendor code. It watches the child and, when a limit is exceeded, kills the
child's whole process group: SIGTERM first, then SIGKILL after a grace period.

Limits (all enforced by the parent):
  total_timeout  wall-clock budget for the whole vendor (setup + load + run)
  idle_timeout   maximum silence (no stdout/stderr) before the child is
                 considered hung
  result_grace   after the child wrote its result file, how long it may take to
                 exit before being killed (lingering non-daemon threads)
"""

import json
import os
import signal
import subprocess
import sys
import threading
import time
import traceback

RESULT_ENV = "BENCH_RESULT_FILE"
PARTIAL_ENV = "BENCH_PARTIAL_FILE"


class Outcome:
    def __init__(self):
        self.returncode = None
        self.reason = None        # None | "total" | "idle" | "result-linger"
        self.kill_mode = None     # None | "terminated" | "killed"
        self.elapsed = 0.0

    @property
    def ok(self):
        return self.reason in (None, "result-linger") and self.returncode in (0, None, -15, -9)

    def __repr__(self):
        return (f"Outcome(rc={self.returncode}, reason={self.reason}, "
                f"kill={self.kill_mode}, elapsed={self.elapsed:.1f}s)")


def _group_alive(pgid):
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def kill_process_group(proc, grace=10.0, log=print):
    """SIGTERM the child's process group, wait `grace` seconds, then SIGKILL.

    The group is always checked after the grace period: even if the group
    leader exited on SIGTERM, descendants (for example a JVM) that ignore it
    are killed with SIGKILL. Returns "exited" | "terminated" | "killed".
    """
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        pgid = proc.pid  # leader already reaped; its group id is still its pid
    if proc.poll() is not None and not _group_alive(pgid):
        return "exited"
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return "exited"
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        if proc.poll() is not None and not _group_alive(pgid):
            return "terminated"
        time.sleep(0.1)
    log(f"  [watchdog] process group {pgid} ignored SIGTERM for {grace:.0f}s, sending SIGKILL")
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        pass
    return "killed"


def run_child(cmd, env=None, total_timeout=3600, idle_timeout=600, grace=10.0,
              result_file=None, result_grace=20.0, log_file=None, log=print, cwd=None):
    """Run `cmd` in its own process group and enforce the limits.

    Child output is streamed to our stdout (and optionally appended to
    `log_file`) while the idle timer is refreshed on every chunk.
    """
    out = Outcome()
    start = time.monotonic()
    proc = subprocess.Popen(cmd, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            start_new_session=True, bufsize=0)
    state = {"last": time.monotonic()}
    logf = open(log_file, "ab") if log_file else None

    def pump():
        try:
            while True:
                chunk = proc.stdout.read1(65536) if hasattr(proc.stdout, "read1") else proc.stdout.read(65536)
                if not chunk:
                    break
                state["last"] = time.monotonic()
                try:
                    sys.stdout.buffer.write(chunk)
                    sys.stdout.buffer.flush()
                except Exception:
                    pass
                if logf:
                    logf.write(chunk)
                    logf.flush()
        except Exception:
            pass

    reader = threading.Thread(target=pump, daemon=True)
    reader.start()

    result_seen_at = None
    try:
        while proc.poll() is None:
            now = time.monotonic()
            if now - start > total_timeout:
                out.reason = "total"
                log(f"\n  [watchdog] total time limit of {total_timeout}s exceeded, terminating")
                break
            if now - state["last"] > idle_timeout:
                out.reason = "idle"
                log(f"\n  [watchdog] no output for {idle_timeout}s, terminating (hung client?)")
                break
            if result_file and os.path.exists(result_file):
                if result_seen_at is None:
                    result_seen_at = now
                elif now - result_seen_at > result_grace:
                    out.reason = "result-linger"
                    log(f"\n  [watchdog] result written but process still alive after {result_grace:.0f}s, terminating")
                    break
            time.sleep(0.2)
        if out.reason:
            out.kill_mode = kill_process_group(proc, grace=grace, log=log)
        out.returncode = proc.returncode if proc.returncode is not None else proc.poll()
        # The leader may have exited normally while descendants (a JVM, a helper
        # started by a client library) are still alive: sweep the whole group so
        # nothing from this vendor survives into the next one.
        if _group_alive(proc.pid):
            mode = kill_process_group(proc, grace=min(grace, 5.0), log=log)
            if out.kill_mode is None and mode in ("terminated", "killed"):
                log(f"  [watchdog] swept leftover processes of the vendor's group ({mode})")
    finally:
        reader.join(timeout=5)
        if logf:
            logf.close()
        out.elapsed = time.monotonic() - start
    return out


# ---------------------------------------------------------------- child side

def _jsonable(obj):
    if isinstance(obj, dict):
        return {str(k): _jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonable(v) for v in obj]
    if isinstance(obj, (int, float, str, bool)) or obj is None:
        return obj
    return str(obj)


def atomic_write_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(_jsonable(data), f)
    os.replace(tmp, path)


def child_main(func):
    """Entry point of the child: run func(), write its result dict to the result file.

    Always ends with os._exit so that lingering non-daemon threads (database
    clients) cannot keep the process alive after the result has been saved.
    """
    result_file = os.environ.get(RESULT_ENV)
    code = 0
    try:
        result = func()
        if result_file:
            atomic_write_json(result_file, result if isinstance(result, dict) else {"error": "no result"})
    except BaseException as e:  # noqa: BLE001 - report everything, including SystemExit
        traceback.print_exc()
        code = 1
        if result_file:
            try:
                atomic_write_json(result_file, {"error": f"{type(e).__name__}: {e}"})
            except Exception:
                pass
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        pass
    os._exit(code)


def read_json(path, default=None):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return default
