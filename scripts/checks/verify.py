"""Run every required project check, failing closed on configuration/tool errors."""

import argparse
import json
import math
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def load_checks(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    checks = data.get("required_checks") if isinstance(data, dict) else None
    if not isinstance(checks, list) or not checks:
        raise ValueError("required_checks must be a nonempty list of real commands")
    names = set()
    for check in checks:
        if not isinstance(check, dict):
            raise ValueError("each required check must be an object")
        name = check.get("name")
        command = check.get("command")
        timeout = check.get("timeout_seconds")
        if not isinstance(name, str) or not name.strip() or name in names:
            raise ValueError("check names must be nonempty and unique")
        names.add(name)
        if (
            not isinstance(command, list)
            or not command
            or any(not isinstance(arg, str) or not arg.strip() for arg in command)
        ):
            raise ValueError(f"{name}: command must be a nonempty list of nonempty arguments")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError(f"{name}: timeout_seconds must be a finite positive number")
    return checks


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", type=Path, default=Path.cwd())
    parser.add_argument("--config", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    project = args.project.resolve()
    try:
        checks = load_checks(args.config or project / ".checks" / "pipeline.json")
    except (OSError, ValueError) as error:
        print(f"Verification configuration error: {error}", file=sys.stderr)
        return 2
    results = []
    started_at = datetime.now(timezone.utc).isoformat()
    for check in checks:
        command = [arg.replace("{python}", sys.executable) for arg in check["command"]]
        started = time.monotonic()
        print(f"Running {check['name']}: {command}", flush=True)
        result = dict(name=check["name"], command=command)
        try:
            completed = subprocess.run(
                command,
                cwd=project,
                timeout=check["timeout_seconds"],
                check=False,
            )
            result.update(
                status="passed" if completed.returncode == 0 else "failed",
                returncode=completed.returncode,
            )
        except subprocess.TimeoutExpired:
            result.update(status="failed", error="timeout")
        except OSError as error:
            result.update(status="failed", error=str(error))
        result["duration_seconds"] = round(time.monotonic() - started, 3)
        results.append(result)
        print(f"{check['name']}: {result['status']}", flush=True)
    passed = all(result["status"] == "passed" for result in results)
    report = args.report or project / "tmp" / "verify-latest.json"
    try:
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(
            json.dumps(
                dict(started_at=started_at, passed=passed, checks=results),
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
    except OSError as error:
        print(f"Cannot save verification report: {error}", file=sys.stderr)
        return 2
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
