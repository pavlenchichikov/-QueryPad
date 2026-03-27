"""CLI entry point for QueryPad."""

import argparse
import sys


def main():
    parser = argparse.ArgumentParser(
        prog="querypad",
        description="QueryPad — SQL notebook with AI assistant",
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="Host to bind (default: 127.0.0.1)"
    )
    parser.add_argument(
        "--port", "-p", type=int, default=8200, help="Port to bind (default: 8200)"
    )
    parser.add_argument(
        "--reload", action="store_true", help="Enable auto-reload for development"
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s 0.1.0"
    )

    args = parser.parse_args()

    try:
        import uvicorn
    except ImportError:
        print("Error: uvicorn is required. Install with: pip install querypad")
        sys.exit(1)

    print(f"  QueryPad v0.1.0")
    print(f"  Starting at http://{args.host}:{args.port}")
    print()

    uvicorn.run(
        "querypad.server:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
