"""Run the test suite inside the Spark image (Java + Spark available, no local Java needed).

Usage (from the repo root, Docker running):
    python scripts/run_spark_tests.py              # all tests
    python scripts/run_spark_tests.py -k minute    # extra arguments go to pytest

Starts a one-off container from the spark-master service with tests/ mounted, installs pytest
and requests (the image ships neither) and runs pytest with Spark's bundled PySpark on the path.
"""
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHONPATH = "/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/opt/app/src"


def main() -> int:
    pytest_args = shlex.join(sys.argv[1:])   # keeps e.g. -k "minute or summary" as one argument
    inner = (
        "pip install -q --disable-pip-version-check pytest requests >/dev/null && "
        f"cd /opt/app && PYTHONPATH={PYTHONPATH} "
        f"python3 -m pytest -p no:cacheprovider -q tests {pytest_args}"
    )
    cmd = [
        "docker", "compose", "run", "--rm", "--no-deps",
        "-v", f"{ROOT / 'tests'}:/opt/app/tests:ro",
        "-v", f"{ROOT / 'pyproject.toml'}:/opt/app/pyproject.toml:ro",
        "--entrypoint", "sh", "spark-master", "-c", inner,
    ]
    return subprocess.call(cmd, cwd=ROOT)


if __name__ == "__main__":
    sys.exit(main())
