from __future__ import annotations

import sys
import time
import urllib.request


def main() -> int:
    if len(sys.argv) < 2:
        print("Использование: python scripts/wait_for_http.py URL [timeout_seconds]")
        return 2
    url = sys.argv[1]
    deadline = time.time() + int(sys.argv[2]) if len(sys.argv) > 2 else time.time() + 60
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=3) as response:
                if 200 <= response.status < 300:
                    print(f"[✓] HTTP-сервис готов: {url}")
                    return 0
        except Exception:
            time.sleep(1)
    print(f"[!] HTTP-сервис не стал доступен: {url}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
