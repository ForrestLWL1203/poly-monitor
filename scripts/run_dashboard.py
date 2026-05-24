#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from poly_monitor.dashboard.server import DashboardConfig, create_server


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the read-only Polymarket observer dashboard.")
    parser.add_argument("--data-dir", type=Path, default=Path("data"), help="Observer data directory.")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host. Default keeps the dashboard local.")
    parser.add_argument("--port", type=int, default=8787, help="Bind port.")
    parser.add_argument("--user", default=os.environ.get("POLY_MONITOR_DASH_USER", "admin"), help="Dashboard username.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    password = os.environ.get("POLY_MONITOR_DASH_PASSWORD", "")
    if not password:
        raise SystemExit("POLY_MONITOR_DASH_PASSWORD must be set before starting the dashboard")
    config = DashboardConfig(
        data_dir=args.data_dir,
        host=args.host,
        port=args.port,
        username=args.user,
        password=password,
        cookie_secret=os.environ.get("POLY_MONITOR_DASH_COOKIE_SECRET", ""),
    )
    server = create_server(config)
    host, port = server.server_address[:2]
    print(f"dashboard listening on http://{host}:{port} data_dir={args.data_dir}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("dashboard stopped", flush=True)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
