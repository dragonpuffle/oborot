import json
import subprocess
import sys
from pathlib import Path

import pytest

VERIFIER = Path(__file__).resolve().parents[2] / "scripts" / "checks" / "verify.py"


def verify(tmp_path, config):
    config_path = tmp_path / "pipeline.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(VERIFIER), "--project", str(tmp_path), "--config", str(config_path)],
        capture_output=True,
        text=True,
        timeout=10,
    )


@pytest.mark.parametrize("config", [{}, {"required_checks": []}])
def test_empty_configuration_is_failure(tmp_path, config):
    assert verify(tmp_path, config).returncode != 0


@pytest.mark.parametrize(
    "command,timeout",
    [
        (["command-that-does-not-exist-oborot"], 1),
        (["{python}", "-c", "raise SystemExit(7)"], 1),
        (["{python}", "-c", "import time; time.sleep(2)"], 0.1),
    ],
)
def test_failed_command_is_failure(tmp_path, command, timeout):
    config = {"required_checks": [{"name": "test", "command": command, "timeout_seconds": timeout}]}
    assert verify(tmp_path, config).returncode != 0


def test_successful_command_is_success(tmp_path):
    config = {
        "required_checks": [
            {
                "name": "test",
                "command": ["{python}", "-c", "print('checked')"],
                "timeout_seconds": 2,
            }
        ]
    }
    result = verify(tmp_path, config)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "check",
    [
        {},
        {"name": "bad", "command": [], "timeout_seconds": 1},
        {"name": "bad", "command": "echo ok", "timeout_seconds": 1},
        {"name": "bad", "command": ["echo"], "timeout_seconds": 0},
        {"name": "bad", "command": ["echo"], "timeout_seconds": True},
    ],
)
def test_invalid_check_configuration_fails(tmp_path, check):
    assert verify(tmp_path, {"required_checks": [check]}).returncode != 0
