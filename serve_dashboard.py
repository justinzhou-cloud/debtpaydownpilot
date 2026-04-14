#!/usr/bin/env python3
"""
Serve the static dashboard over HTTP (no need to `cd dashboard`).

Run from the snowflake-connection repo root:

  .venv/bin/python serve_dashboard.py

Then open the URL printed (default port 8765).

If 8765 is busy (another server or a previous serve_dashboard), the script tries
8766, 8767, … automatically. Or set a port explicitly:

  PORT=9000 python3 serve_dashboard.py
  python3 serve_dashboard.py --port 9000

If you see "no such file or directory: dashboard", you were probably in the
wrong folder (e.g. Projects/ instead of Projects/snowflake-connection/).
This script always uses the dashboard/ folder next to this file.
"""

from __future__ import annotations

import argparse
import errno
import http.server
import os
import socketserver
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DASH = ROOT / "dashboard"
DEFAULT_PORT = 8765
MAX_PORT_TRIES = 32


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve snowflake-connection dashboard over HTTP")
    parser.add_argument(
        "--port",
        "-p",
        type=int,
        default=None,
        help=f"Port to bind (default: {DEFAULT_PORT} or env PORT)",
    )
    args = parser.parse_args()
    base = args.port if args.port is not None else int(os.environ.get("PORT", DEFAULT_PORT))

    if not DASH.is_dir():
        raise SystemExit(
            f"Missing directory: {DASH}\n"
            "The dashboard folder should live next to serve_dashboard.py "
            f"(repo root: {ROOT})."
        )
    if not (DASH / "index.html").is_file():
        raise SystemExit(
            f"Missing {DASH / 'index.html'}. Restore dashboard assets from the repo, "
            "then run: python build_dashboard.py"
        )

    class Handler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=str(DASH), **kwargs)

        def end_headers(self) -> None:
            # Avoid stale dashboard after `build_dashboard.py` — browsers cache data.js aggressively.
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            super().end_headers()

    # Bind only to loopback; avoid empty responses from wrong interface / IPv6 mismatch.
    host = "127.0.0.1"
    socketserver.TCPServer.allow_reuse_address = True

    httpd = None
    port = base
    for offset in range(MAX_PORT_TRIES):
        candidate = base + offset
        try:
            httpd = socketserver.TCPServer((host, candidate), Handler)
            port = candidate
            break
        except OSError as e:
            if e.errno not in (errno.EADDRINUSE, getattr(errno, "WSAEADDRINUSE", -1)):
                raise
            if offset == MAX_PORT_TRIES - 1:
                raise SystemExit(
                    f"No free port found from {base} to {base + MAX_PORT_TRIES - 1}. "
                    f"Stop the other process using the port or run: python3 serve_dashboard.py --port <other>"
                ) from e

    if port != base:
        print(f"Port {base} was in use; using {port} instead.\n")

    with httpd:
        print(f"Serving {DASH}")
        print(f"  http://{host}:{port}/")
        print("Keep this terminal open while browsing — closing it stops the server.")
        print("If the browser shows ERR_EMPTY_RESPONSE, the server is not running on that port.")
        httpd.serve_forever()


if __name__ == "__main__":
    main()
