from __future__ import annotations

import json
import sys
import urllib.request


def check_url(name: str, url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            ok = 200 <= response.status < 300
            print(f"[{'✓' if ok else '!'}] {name}: HTTP {response.status}")
            if name == "API health":
                print(json.dumps(json.loads(response.read().decode("utf-8")), ensure_ascii=False, indent=2))
            return ok
    except Exception as exc:
        print(f"[!] {name}: недоступно: {exc}")
        return False


def main() -> int:
    checks = [
        check_url("API health", "http://localhost:8000/health"),
        check_url("Prometheus", "http://localhost:9090/-/healthy"),
        check_url("Grafana", "http://localhost:3000/api/health"),
    ]
    return 0 if all(checks) else 1


if __name__ == "__main__":
    raise SystemExit(main())
