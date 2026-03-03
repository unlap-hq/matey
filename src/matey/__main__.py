import sys


def main() -> int:
    sys.stderr.write("matey CLI not wired yet; lockfile module is available.\n")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
