"""Which JDK the ArcadeDB benchmarks may run on: Eclipse Temurin 25 only.

Project rule: published numbers come from Temurin 25 with
-XX:+UseCompactObjectHeaders. GraalVM is never used (its default Graal JIT was
slower and erratic on the vectorised OLAP paths), and neither is any other JDK.
"""

import glob
import os
import subprocess

JVM_FLAGS = "-XX:+UseCompactObjectHeaders"


def _release(home):
    info = {}
    try:
        with open(os.path.join(home, "release")) as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    info[k] = v.strip('"')
    except OSError:
        pass
    return info


def check_jdk(home):
    """Returns (ok, reason). ok only for Eclipse Temurin 25.x."""
    java = os.path.join(home, "bin", "java")
    if not os.path.exists(java):
        return False, f"{home} has no bin/java"
    rel = _release(home)
    text = " ".join(rel.get(k, "") for k in ("IMPLEMENTOR", "IMPLEMENTOR_VERSION", "JAVA_VERSION"))
    if not text.strip():
        out = subprocess.run([java, "-version"], capture_output=True, text=True).stderr
        text = out
    if "graal" in text.lower() or "graal" in home.lower():
        return False, "GraalVM must not be used for the benchmarks (Temurin 25 only)"
    if "temurin" not in text.lower():
        return False, f"{home} is not Eclipse Temurin ({rel.get('IMPLEMENTOR', 'unknown vendor')})"
    if not rel.get("JAVA_VERSION", "").startswith("25"):
        return False, f"{home} is Java {rel.get('JAVA_VERSION', '?')}, Temurin 25 is required"
    return True, "ok"


def candidates():
    env = os.environ.get("LDBC_JAVA_HOME")
    if env:
        yield env
    patterns = [
        os.path.expanduser("~/Library/Java/JavaVirtualMachines/*/Contents/Home"),
        "/Library/Java/JavaVirtualMachines/*/Contents/Home",
        "/usr/lib/jvm/*",
        os.path.expanduser("~/.sdkman/candidates/java/*"),
    ]
    for pat in patterns:
        for p in sorted(glob.glob(pat)):
            yield p
    if os.environ.get("JAVA_HOME"):
        yield os.environ["JAVA_HOME"]


def require_temurin25(home=None):
    """Return a validated Temurin 25 home; raise SystemExit with a clear reason otherwise."""
    if home:
        ok, why = check_jdk(home)
        if not ok:
            raise SystemExit(f"Refusing to run: {why}")
        return home
    for c in candidates():
        ok, _ = check_jdk(c)
        if ok:
            return c
    raise SystemExit("Temurin 25 not found. Install it (https://adoptium.net/temurin/releases/?version=25) "
                     "and pass --java-home or set LDBC_JAVA_HOME. GraalVM is not allowed.")


def assert_not_graalvm(java_cmd="java"):
    """Cheap guard for code that shells out to `java` from PATH."""
    out = subprocess.run([java_cmd, "-version"], capture_output=True, text=True).stderr.lower()
    if "graal" in out:
        raise SystemExit("Refusing to run: GraalVM is not allowed for the benchmarks (use Temurin 25).")


def env_for(home):
    """Environment that makes `java`/`javac` resolve to `home` for child processes."""
    env = dict(os.environ)
    env["JAVA_HOME"] = home
    env["PATH"] = os.path.join(home, "bin") + os.pathsep + env.get("PATH", "")
    env["ARCADEDB_JVM_FLAGS"] = JVM_FLAGS
    return env
