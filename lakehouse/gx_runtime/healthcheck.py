"""Container/Kubernetes-compatible liveness and readiness probe command."""

from __future__ import annotations

import os
import sys
from urllib.request import urlopen


def check(kind: str) -> int:
    if kind not in {"live", "ready"}:
        print("probe must be 'live' or 'ready'", file=sys.stderr)
        return 2
    port = int(os.getenv("GX_HTTP_PORT", "8090"))
    timeout = float(os.getenv("GX_HEALTHCHECK_TIMEOUT_SECONDS", "3"))
    try:
        with urlopen(f"http://127.0.0.1:{port}/health/{kind}", timeout=timeout) as response:
            return 0 if response.status == 200 else 1
    except Exception as exc:
        print(f"GX {kind} probe failed: {exc}", file=sys.stderr)
        return 1


def main() -> None:
    kind = sys.argv[1] if len(sys.argv) > 1 else "ready"
    raise SystemExit(check(kind))


if __name__ == "__main__":
    main()
