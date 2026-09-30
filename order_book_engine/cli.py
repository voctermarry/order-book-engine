"""Command line entry point for order-book-engine."""

from __future__ import annotations

import argparse
import sys

from . import __version__
from .replay import replay


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="order-book-engine", description="Deterministic limit order book matching and execution analytics")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("version", help="print the current version")
    sub.add_parser("replay", help="replay JSON Lines order events from standard input")
    args = parser.parse_args(argv)

    if args.command == "version":
        print(__version__)
        return 0

    if args.command == "replay":
        return replay()

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
