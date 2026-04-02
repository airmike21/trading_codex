#!/usr/bin/env python3
from __future__ import annotations

import argparse
import socket
import threading
from dataclasses import dataclass
from typing import Callable


DEFAULT_HOST = "172.26.192.1"
DEFAULT_PORT = 7497
DEFAULT_CLIENT_ID = 999
DEFAULT_TIMEOUT_SECONDS = 10.0
EXIT_SUCCESS = 0
EXIT_FAILURE = 2
IBAPI_IMPORT_UNAVAILABLE = "IBApi import unavailable in repo environment."
_INFORMATIONAL_IB_ERROR_CODES = {
    1100,
    1101,
    1102,
    2103,
    2104,
    2105,
    2106,
    2107,
    2108,
    2158,
}
_FAST_FAIL_IB_ERROR_CODES = {
    326,
    502,
    503,
    504,
}


@dataclass(frozen=True)
class TcpProbeResult:
    ok: bool
    detail: str


@dataclass(frozen=True)
class HandshakeProbeResult:
    ok: bool
    detail: str
    next_valid_id: int | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only IBKR Paper TWS connectivity smoke test. "
            "Checks raw TCP reachability first, then attempts an IBKR API handshake when IBApi is available."
        ),
        epilog=(
            "Example:\n"
            "  .venv/bin/python scripts/test_ibkr_connection.py "
            "--host 172.26.192.1 --port 7497 --client-id 999"
        ),
    )
    parser.add_argument("--host", type=str, default=DEFAULT_HOST, help="TWS/Gateway host.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="TWS/Gateway socket port.")
    parser.add_argument("--client-id", type=int, default=DEFAULT_CLIENT_ID, help="IBKR API client id for the handshake probe.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="Timeout in seconds for both the TCP probe and the IBKR API handshake.",
    )
    return parser


def _load_ibapi_classes() -> tuple[type[object], type[object]]:
    try:
        from ibapi.client import EClient
        from ibapi.wrapper import EWrapper
    except ImportError as exc:  # pragma: no cover - depends on local environment
        raise ImportError(IBAPI_IMPORT_UNAVAILABLE) from exc
    return EClient, EWrapper


def probe_tcp_connectivity(*, host: str, port: int, timeout: float) -> TcpProbeResult:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return TcpProbeResult(ok=True, detail=f"Connected to {host}:{port}.")
    except OSError as exc:
        return TcpProbeResult(ok=False, detail=f"{exc.__class__.__name__}: {exc}")


def probe_ibkr_handshake(
    *,
    host: str,
    port: int,
    client_id: int,
    timeout: float,
    ibapi_loader: Callable[[], tuple[type[object], type[object]]] = _load_ibapi_classes,
) -> HandshakeProbeResult:
    try:
        eclient_cls, ewrapper_cls = ibapi_loader()
    except ImportError as exc:
        return HandshakeProbeResult(ok=False, detail=str(exc) or IBAPI_IMPORT_UNAVAILABLE)

    done = threading.Event()
    next_valid_id: dict[str, int] = {}
    errors: list[str] = []

    class HandshakeApp(ewrapper_cls, eclient_cls):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            eclient_cls.__init__(self, self)

        def nextValidId(self, orderId: int) -> None:  # noqa: N802 - IB API callback name
            next_valid_id["value"] = int(orderId)
            done.set()
            try:
                self.disconnect()
            except Exception:
                pass

        def error(  # type: ignore[override]
            self,
            reqId: int,
            errorCode: int,
            errorString: str,
            advancedOrderRejectJson: str = "",
        ) -> None:  # noqa: N803 - IB API callback name
            del advancedOrderRejectJson
            message = f"reqId={reqId} code={errorCode} message={errorString}"
            if int(errorCode) in _INFORMATIONAL_IB_ERROR_CODES:
                return
            errors.append(message)
            if int(errorCode) in _FAST_FAIL_IB_ERROR_CODES:
                done.set()

        def connectionClosed(self) -> None:  # noqa: N802 - IB API callback name
            if "value" in next_valid_id:
                return
            errors.append("connection closed before nextValidId")
            done.set()

    app = HandshakeApp()
    thread: threading.Thread | None = None
    try:
        app.connect(host, port, clientId=client_id)
        thread = threading.Thread(target=app.run, daemon=True, name="ibkr-handshake-probe")
        thread.start()
        completed = done.wait(timeout)
        if "value" in next_valid_id:
            return HandshakeProbeResult(
                ok=True,
                detail="Received nextValidId from IBKR API.",
                next_valid_id=next_valid_id["value"],
            )
        if completed and errors:
            return HandshakeProbeResult(ok=False, detail=errors[0])
        detail = f"Timed out after {timeout:g} seconds waiting for nextValidId."
        if errors:
            detail = f"{detail} Last error: {errors[0]}"
        return HandshakeProbeResult(ok=False, detail=detail)
    except Exception as exc:  # pragma: no cover - exercised only with a real or fake IBApi implementation
        return HandshakeProbeResult(ok=False, detail=f"{exc.__class__.__name__}: {exc}")
    finally:
        try:
            app.disconnect()
        except Exception:
            pass
        if thread is not None:
            thread.join(timeout=1.0)


def _print_report(*, tcp_result: TcpProbeResult, handshake_result: HandshakeProbeResult, exit_code: int) -> None:
    print(f"TCP connect {'OK' if tcp_result.ok else 'FAILED'}: {tcp_result.detail}")
    print(f"IBKR handshake {'OK' if handshake_result.ok else 'FAILED'}: {handshake_result.detail}")
    if handshake_result.next_valid_id is not None:
        print(f"nextValidId: {handshake_result.next_valid_id}")
    else:
        print("nextValidId: not received")
    print(f"Exit status: {exit_code}")


def main(
    argv: list[str] | None = None,
    *,
    tcp_probe: Callable[..., TcpProbeResult] = probe_tcp_connectivity,
    handshake_probe: Callable[..., HandshakeProbeResult] = probe_ibkr_handshake,
) -> int:
    args = build_parser().parse_args(argv)
    tcp_result = tcp_probe(host=args.host, port=args.port, timeout=args.timeout)
    if tcp_result.ok:
        handshake_result = handshake_probe(
            host=args.host,
            port=args.port,
            client_id=args.client_id,
            timeout=args.timeout,
        )
    else:
        handshake_result = HandshakeProbeResult(ok=False, detail="Skipped because TCP connectivity failed.")

    exit_code = EXIT_SUCCESS if tcp_result.ok and handshake_result.ok else EXIT_FAILURE
    _print_report(tcp_result=tcp_result, handshake_result=handshake_result, exit_code=exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
