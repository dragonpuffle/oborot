from dataclasses import replace

import pytest

from common.config import Settings


@pytest.mark.parametrize(
    "change",
    [
        {"block_ms": 0},
        {"batch_size": 0},
        {"claim_idle_ms": -1},
        {"poll_interval": 0},
        {"poll_interval": float("nan")},
        {"stream": ""},
        {"dead_letter_stream": Settings.stream},
    ],
)
def test_invalid_worker_settings_fail_at_startup(change):
    with pytest.raises(ValueError):
        replace(Settings(), **change)


def test_environment_settings(monkeypatch):
    monkeypatch.setenv("REDIS_GROUP", "custom-group")
    monkeypatch.setenv("CLAIM_IDLE_MS", "42")
    assert Settings.from_env().group == "custom-group"
    assert Settings.from_env().claim_idle_ms == 42
    assert Settings.from_env().consumer != Settings.from_env().consumer
