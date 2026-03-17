#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import threading
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

from dashboard_parser import discover_runs, payload_etag


TOOLS_DIR = Path(__file__).resolve().parent
LOADTEST_DIR = TOOLS_DIR.parent
DASHBOARD_DIR = TOOLS_DIR / "dashboard"


def build_handler(scan_root: Path):
    class DashboardHandler(BaseHTTPRequestHandler):
        server_version = "BlnkBenchmarkDashboard/1.0"

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            path = parsed.path

            if path in {"/", ""}:
                self.redirect("/dashboard/")
                return
            if path == "/api/runs":
                self.send_json({"runs": discover_runs(scan_root)})
                return
            if path.startswith("/api/runs/"):
                run_id = unquote(path.removeprefix("/api/runs/"))
                self.handle_run(run_id)
                return
            if path == "/dashboard" or path == "/dashboard/":
                self.serve_file(DASHBOARD_DIR / "index.html")
                return
            if path.startswith("/dashboard/"):
                relative = Path(path.removeprefix("/dashboard/"))
                target = (DASHBOARD_DIR / relative).resolve()
                if DASHBOARD_DIR not in target.parents and target != DASHBOARD_DIR:
                    self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                    return
                self.serve_file(target)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Not found")

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def handle_run(self, run_id: str) -> None:
            for run in discover_runs(scan_root):
                if run.get("id") == run_id:
                    self.send_json(run)
                    return
            self.send_error(HTTPStatus.NOT_FOUND, "Run not found")

        def send_json(self, payload: object) -> None:
            body = json.dumps(payload).encode("utf-8")
            etag = payload_etag(payload)
            if self.headers.get("If-None-Match") == etag:
                self.send_response(HTTPStatus.NOT_MODIFIED)
                self.send_header("ETag", etag)
                self.end_headers()
                return
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("ETag", etag)
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def serve_file(self, path: Path) -> None:
            if not path.exists() or not path.is_file():
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")
                return
            content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
            body = path.read_bytes()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", f"{content_type}; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                return

        def redirect(self, location: str) -> None:
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", location)
            self.end_headers()

    return DashboardHandler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the load-test dashboard and discovered load-test data."
    )
    parser.add_argument(
        "--root",
        default=str(LOADTEST_DIR),
        help="Directory to scan for load-test JSON, CSV, and NDJSON files.",
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind.")
    parser.add_argument(
        "--no-open",
        action="store_true",
        help="Do not automatically open the dashboard in the default browser.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    scan_root = Path(args.root).resolve()
    if not scan_root.exists():
        print(f"Scan root does not exist: {scan_root}", file=sys.stderr)
        return 1
    if not DASHBOARD_DIR.exists():
        print(f"Dashboard assets are missing: {DASHBOARD_DIR}", file=sys.stderr)
        return 1

    server = ThreadingHTTPServer((args.host, args.port), build_handler(scan_root))
    url = f"http://{args.host}:{args.port}/dashboard/"

    if not args.no_open:
        threading.Timer(0.2, lambda: webbrowser.open(url)).start()

    print(f"Benchmark dashboard serving {scan_root}")
    print(f"Open {url}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down dashboard server.")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
