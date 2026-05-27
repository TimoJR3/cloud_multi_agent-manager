import sys
import urllib.request


def main() -> int:
    if len(sys.argv) != 2:
        print("Нужно передать URL healthcheck")
        return 2
    try:
        with urllib.request.urlopen(sys.argv[1], timeout=3) as response:
            return 0 if 200 <= response.status < 300 else 1
    except Exception as exc:
        print(f"Проверка недоступна: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
