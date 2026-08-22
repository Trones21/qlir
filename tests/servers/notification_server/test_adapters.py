# tests/servers/notification_server/test_adapters.py

import json

import pytest

from qlir.servers.notification_server.adapters.console import ConsoleAdapter
from qlir.servers.notification_server.adapters.email import EmailAdapter
from qlir.servers.notification_server.adapters.file import FileAdapter
from qlir.servers.notification_server.adapters.registry import SINK_TYPES
from qlir.servers.notification_server.adapters.webhook import WebhookAdapter


def test_console_adapter_writes_the_payload(capsys):
    ConsoleAdapter().send({"trigger": "macd_reversal"})
    assert "macd_reversal" in capsys.readouterr().out


def test_console_adapter_rejects_an_unknown_stream():
    with pytest.raises(ValueError, match="stdout"):
        ConsoleAdapter(stream="carrier-pigeon")


def test_file_adapter_appends_one_json_object_per_line(tmp_path):
    path = tmp_path / "nested" / "alerts.jsonl"
    adapter = FileAdapter(path)
    adapter.send({"trigger": "a"})
    adapter.send({"trigger": "b"})

    lines = path.read_text().strip().splitlines()
    assert [json.loads(x)["trigger"] for x in lines] == ["a", "b"]


def test_file_adapter_creates_parent_directories(tmp_path):
    path = tmp_path / "deep" / "deeper" / "alerts.jsonl"
    FileAdapter(path).send({"x": 1})
    assert path.exists()


def test_email_adapter_requires_a_recipient():
    with pytest.raises(ValueError, match="to_addrs"):
        EmailAdapter(
            smtp_host="smtp.example.com", smtp_port=587,
            from_addr="a@example.com", to_addrs=[],
        )


def test_email_adapter_builds_and_sends_a_message(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, host, port, timeout=None):
            sent["host"] = host
            sent["port"] = port

        def ehlo(self): pass
        def starttls(self): sent["tls"] = True
        def login(self, u, p): sent["login"] = (u, p)
        def send_message(self, msg): sent["msg"] = msg
        def quit(self): pass

    monkeypatch.setattr("smtplib.SMTP", FakeSMTP)

    EmailAdapter(
        smtp_host="smtp.example.com", smtp_port=587,
        from_addr="from@example.com", to_addrs=["to@example.com"],
        username="u", password="p",
    ).send({"trigger": "macd_reversal", "close": 1.0})

    assert sent["host"] == "smtp.example.com"
    assert sent["tls"] is True
    assert sent["login"] == ("u", "p")
    msg = sent["msg"]
    assert msg["To"] == "to@example.com"
    assert "macd_reversal" in msg["Subject"]
    assert "macd_reversal" in msg.get_content()


def test_email_subject_falls_back_when_payload_has_no_trigger(monkeypatch):
    sent = {}

    class FakeSMTP:
        def __init__(self, *a, **k): pass
        def ehlo(self): pass
        def starttls(self): pass
        def send_message(self, msg): sent["msg"] = msg
        def quit(self): pass

    monkeypatch.setattr("smtplib.SMTP", FakeSMTP)
    EmailAdapter(
        smtp_host="h", smtp_port=1, from_addr="f@x.com", to_addrs=["t@x.com"],
    ).send({"some": "payload"})

    assert sent["msg"]["Subject"] == "[qlir] alert"


def test_webhook_adapter_posts_json(monkeypatch):
    captured = {}

    class FakeResp:
        ok = True
        status_code = 200
        text = ""

    def fake_request(method, url, json=None, headers=None, timeout=None):
        captured.update(method=method, url=url, json=json, headers=headers)
        return FakeResp()

    monkeypatch.setattr("requests.request", fake_request)

    WebhookAdapter("https://example.com/hook", headers={"X-Source": "qlir"}).send({"a": 1})

    assert captured["method"] == "POST"
    assert captured["url"] == "https://example.com/hook"
    assert captured["json"] == {"a": 1}
    assert captured["headers"]["X-Source"] == "qlir"


def test_webhook_adapter_raises_on_a_non_2xx_response(monkeypatch):
    class FakeResp:
        ok = False
        status_code = 500
        text = "boom"

    monkeypatch.setattr("requests.request", lambda *a, **k: FakeResp())

    with pytest.raises(RuntimeError, match="500"):
        WebhookAdapter("https://example.com/hook").send({"a": 1})


# --------------------------------------------------------------------------
# Registry contract -- guards the setup guidance the skill and errors depend on
# --------------------------------------------------------------------------

def test_every_sink_type_documents_its_setup():
    for name, sink_type in SINK_TYPES.items():
        assert sink_type.setup_steps, f"{name} has no setup_steps"
        assert sink_type.summary, f"{name} has no summary"
        assert sink_type.name == name


def test_env_keys_are_declared_as_config_keys():
    for name, sink_type in SINK_TYPES.items():
        known = set(sink_type.required) | set(sink_type.optional)
        unknown = set(sink_type.env_keys) - known
        assert not unknown, f"{name}: env_keys {unknown} are not declared keys"
