# tests/servers/notification_server/test_config.py

import pytest

from qlir.servers.notification_server.adapters.registry import SinkConfigError
from qlir.servers.notification_server.config import (
    build_adapters,
    config_path,
    console_fallback,
    load_config,
    parse_config,
)

import tomllib


def _cfg(text: str, source="test.toml"):
    return parse_config(tomllib.loads(text), source=source)


MINIMAL = """
[sinks.out]
type = "console"

[routes]
"qlir-events" = ["out"]
"""


def test_parses_a_minimal_config():
    cfg = _cfg(MINIMAL)
    assert cfg.sinks["out"].type == "console"
    assert cfg.routes["qlir-events"] == ["out"]
    assert cfg.referenced_sinks == {"out"}


def test_one_outbox_can_fan_out_to_several_sinks(tmp_path):
    cfg = _cfg(f"""
[sinks.a]
type = "console"

[sinks.b]
type = "file"
path = "{tmp_path / 'x.jsonl'}"

[routes]
"qlir-tradable-human" = ["a", "b"]
""")
    adapters = build_adapters(cfg)
    assert len(adapters["qlir-tradable-human"]) == 2


def test_a_single_sink_name_may_be_given_as_a_bare_string():
    cfg = _cfg("""
[sinks.out]
type = "console"

[routes]
"qlir-events" = "out"
""")
    assert cfg.routes["qlir-events"] == ["out"]


# --------------------------------------------------------------------------
# The scoped-validation rule: only what you selected has to work.
# --------------------------------------------------------------------------

UNREFERENCED_TELEGRAM = """
[sinks.out]
type = "console"

[sinks.tg]
type = "telegram"
bot_token_env = "QLIR_TEST_TOKEN_THAT_IS_NOT_SET"
chat_id_env = "QLIR_TEST_CHAT_THAT_IS_NOT_SET"

[routes]
"qlir-events" = ["out"]
"""


def test_unreferenced_sink_with_missing_env_is_inert(monkeypatch):
    monkeypatch.delenv("QLIR_TEST_TOKEN_THAT_IS_NOT_SET", raising=False)
    cfg = _cfg(UNREFERENCED_TELEGRAM)
    adapters = build_adapters(cfg)  # must not raise
    assert set(adapters) == {"qlir-events"}
    assert "tg" not in cfg.referenced_sinks


def test_referenced_sink_with_missing_env_is_a_hard_error(monkeypatch):
    monkeypatch.delenv("QLIR_TEST_TOKEN_THAT_IS_NOT_SET", raising=False)
    cfg = _cfg(UNREFERENCED_TELEGRAM.replace(
        '"qlir-events" = ["out"]', '"qlir-events" = ["out", "tg"]'
    ))
    with pytest.raises(SinkConfigError) as e:
        build_adapters(cfg)
    msg = str(e.value)
    assert "QLIR_TEST_TOKEN_THAT_IS_NOT_SET" in msg
    assert "BotFather" in msg  # the error carries setup steps


def test_all_broken_sinks_are_reported_in_one_pass(monkeypatch):
    monkeypatch.delenv("QLIR_TEST_TOKEN_THAT_IS_NOT_SET", raising=False)
    cfg = _cfg("""
[sinks.tg]
type = "telegram"
bot_token_env = "QLIR_TEST_TOKEN_THAT_IS_NOT_SET"
chat_id_env = "QLIR_TEST_TOKEN_THAT_IS_NOT_SET"

[sinks.mail]
type = "email"
smtp_host = "smtp.example.com"

[routes]
"qlir-events" = ["tg", "mail"]
""")
    with pytest.raises(SinkConfigError) as e:
        build_adapters(cfg)
    msg = str(e.value)
    assert "[tg]" in msg and "[mail]" in msg
    assert "from_addr" in msg  # the email sink's specific missing keys


# --------------------------------------------------------------------------
# Malformed config
# --------------------------------------------------------------------------

def test_unknown_sink_type_is_rejected_even_when_unreferenced():
    with pytest.raises(SinkConfigError, match="unknown sink type"):
        _cfg("""
[sinks.weird]
type = "carrier-pigeon"

[sinks.out]
type = "console"

[routes]
"qlir-events" = ["out"]
""")


def test_route_to_an_undefined_sink_is_rejected():
    with pytest.raises(SinkConfigError, match="undefined sink"):
        _cfg("""
[sinks.out]
type = "console"

[routes]
"qlir-events" = ["nope"]
""")


def test_sink_without_a_type_is_rejected():
    with pytest.raises(SinkConfigError, match="missing 'type'"):
        _cfg("""
[sinks.out]
pretty = true

[routes]
"qlir-events" = ["out"]
""")


def test_config_with_no_routes_is_rejected():
    with pytest.raises(SinkConfigError, match="at least one route"):
        _cfg("""
[sinks.out]
type = "console"
""")


def test_config_with_no_sinks_is_rejected():
    with pytest.raises(SinkConfigError, match="at least one sink"):
        _cfg("""
[routes]
"qlir-events" = ["out"]
""")


# --------------------------------------------------------------------------
# Fallback + discovery
# --------------------------------------------------------------------------

def test_console_fallback_routes_every_declared_outbox():
    cfg = console_fallback(["qlir-events", "qlir-ops"])
    assert cfg.routes == {"qlir-events": ["console"], "qlir-ops": ["console"]}
    adapters = build_adapters(cfg)
    assert set(adapters) == {"qlir-events", "qlir-ops"}


def test_no_config_and_no_declared_outboxes_explains_itself(monkeypatch, tmp_path):
    monkeypatch.delenv("QLIR_NOTIFICATIONS_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    with pytest.raises(SinkConfigError, match="analysis_outboxes.json"):
        load_config(declared_outboxes=[])


def test_config_is_found_by_walking_up_from_a_subdirectory(monkeypatch, tmp_path):
    """
    start_all_simple.sh runs from src/qlir/servers, so a repo-root config must
    still be found from a nested cwd.
    """
    monkeypatch.delenv("QLIR_NOTIFICATIONS_CONFIG", raising=False)
    (tmp_path / "notifications.toml").write_text(MINIMAL)
    nested = tmp_path / "src" / "qlir" / "servers"
    nested.mkdir(parents=True)
    monkeypatch.chdir(nested)

    found = config_path()
    assert found == tmp_path / "notifications.toml"


def test_explicit_env_var_wins_over_the_upward_walk(monkeypatch, tmp_path):
    (tmp_path / "notifications.toml").write_text(MINIMAL)
    explicit = tmp_path / "elsewhere.toml"
    explicit.write_text(MINIMAL)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("QLIR_NOTIFICATIONS_CONFIG", str(explicit))

    assert config_path() == explicit
