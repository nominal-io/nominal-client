#!/usr/bin/env python3
"""Serve a built HTML report on localhost.

  python serve_report.py <dir> [--file report.html] [--port N] [--open]

Binds 127.0.0.1 on a free port (or --port), serves <dir>, and prints the URL. Runs
until interrupted, so launch it in the background; the report is self-contained, so
`file://` works too if you'd rather not run a server.
"""
from __future__ import annotations

import argparse
import http.server
import socketserver
import sys
import webbrowser
from functools import partial
from pathlib import Path


class _QuietHandler(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *args: object) -> None:  # keep the background process quiet
        pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("directory")
    ap.add_argument("--file", default=None, help="report filename to surface in the printed URL")
    ap.add_argument("--port", type=int, default=0, help="0 picks a free port")
    ap.add_argument("--open", action="store_true", help="open the report in a browser")
    args = ap.parse_args()

    directory = str(Path(args.directory).resolve())
    handler = partial(_QuietHandler, directory=directory)
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("127.0.0.1", args.port), handler) as httpd:
        port = httpd.server_address[1]
        base = f"http://localhost:{port}/"
        url = base + (args.file or "")
        print(f"Serving {directory}")
        print(f"Report: {url}" if args.file else f"Open:   {base}")
        sys.stdout.flush()
        if args.open:
            webbrowser.open(url)
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
