from __future__ import annotations

from scripts import test_ibkr_connection


class _FakeSocket:
    def __enter__(self) -> "_FakeSocket":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        del exc_type, exc, tb
        return False


def test_build_parser_defaults() -> None:
    parser = test_ibkr_connection.build_parser()
    args = parser.parse_args([])

    assert args.host == "172.26.192.1"
    assert args.port == 7497
    assert args.client_id == 999
    assert args.timeout == 10.0


def test_probe_tcp_connectivity_reports_success(monkeypatch) -> None:
    def fake_create_connection(address, timeout):
        assert address == ("172.26.192.1", 7497)
        assert timeout == 10.0
        return _FakeSocket()

    monkeypatch.setattr(test_ibkr_connection.socket, "create_connection", fake_create_connection)

    result = test_ibkr_connection.probe_tcp_connectivity(
        host="172.26.192.1",
        port=7497,
        timeout=10.0,
    )

    assert result.ok is True
    assert result.detail == "Connected to 172.26.192.1:7497."


def test_probe_tcp_connectivity_reports_failure(monkeypatch) -> None:
    def fake_create_connection(address, timeout):
        del address, timeout
        raise OSError("connection refused")

    monkeypatch.setattr(test_ibkr_connection.socket, "create_connection", fake_create_connection)

    result = test_ibkr_connection.probe_tcp_connectivity(
        host="172.26.192.1",
        port=7497,
        timeout=10.0,
    )

    assert result.ok is False
    assert result.detail == "OSError: connection refused"


def test_probe_ibkr_handshake_reports_import_unavailable() -> None:
    result = test_ibkr_connection.probe_ibkr_handshake(
        host="172.26.192.1",
        port=7497,
        client_id=999,
        timeout=10.0,
        ibapi_loader=lambda: (_ for _ in ()).throw(ImportError(test_ibkr_connection.IBAPI_IMPORT_UNAVAILABLE)),
    )

    assert result.ok is False
    assert result.detail == test_ibkr_connection.IBAPI_IMPORT_UNAVAILABLE
    assert result.next_valid_id is None


def test_main_reports_successful_tcp_and_handshake(capsys) -> None:
    rc = test_ibkr_connection.main(
        [],
        tcp_probe=lambda **_: test_ibkr_connection.TcpProbeResult(ok=True, detail="Connected to 172.26.192.1:7497."),
        handshake_probe=lambda **_: test_ibkr_connection.HandshakeProbeResult(
            ok=True,
            detail="Received nextValidId from IBKR API.",
            next_valid_id=12345,
        ),
    )

    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [
        "TCP connect OK: Connected to 172.26.192.1:7497.",
        "IBKR handshake OK: Received nextValidId from IBKR API.",
        "nextValidId: 12345",
        "Exit status: 0",
    ]


def test_main_skips_handshake_when_tcp_fails(capsys) -> None:
    handshake_calls: list[dict[str, object]] = []

    def fake_handshake_probe(**kwargs):
        handshake_calls.append(kwargs)
        return test_ibkr_connection.HandshakeProbeResult(ok=True, detail="unexpected")

    rc = test_ibkr_connection.main(
        [],
        tcp_probe=lambda **_: test_ibkr_connection.TcpProbeResult(ok=False, detail="OSError: connection refused"),
        handshake_probe=fake_handshake_probe,
    )

    assert rc == 2
    assert handshake_calls == []
    assert capsys.readouterr().out.splitlines() == [
        "TCP connect FAILED: OSError: connection refused",
        "IBKR handshake FAILED: Skipped because TCP connectivity failed.",
        "nextValidId: not received",
        "Exit status: 2",
    ]


def test_main_fails_closed_when_ibapi_is_unavailable(capsys) -> None:
    rc = test_ibkr_connection.main(
        [],
        tcp_probe=lambda **_: test_ibkr_connection.TcpProbeResult(ok=True, detail="Connected to 172.26.192.1:7497."),
        handshake_probe=lambda **_: test_ibkr_connection.HandshakeProbeResult(
            ok=False,
            detail=test_ibkr_connection.IBAPI_IMPORT_UNAVAILABLE,
        ),
    )

    assert rc == 2
    assert capsys.readouterr().out.splitlines() == [
        "TCP connect OK: Connected to 172.26.192.1:7497.",
        f"IBKR handshake FAILED: {test_ibkr_connection.IBAPI_IMPORT_UNAVAILABLE}",
        "nextValidId: not received",
        "Exit status: 2",
    ]
