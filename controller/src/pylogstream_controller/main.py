import asyncio
import sys
import argparse
from pylogstream_controller.config import load_config
from pylogstream_controller.server import ControllerServer

def main():
    parser = argparse.ArgumentParser(description="PyLogStream Controller")
    parser.add_argument("--config", default="config.yaml", help="Path to config yaml file")
    parser.add_argument("--host", help="Controller host")
    parser.add_argument("--port", type=int, help="Controller port")
    args = parser.parse_args()

    host = "127.0.0.1"
    port = 9093

    try:
        cfg = load_config(args.config)
        host = cfg.controller.host
        port = cfg.controller.port
    except Exception as e:
        print(f"Warning: Could not load config file {args.config} ({e}). Using default host/port.")

    if args.host:
        host = args.host
    if args.port:
        port = args.port

    server = ControllerServer(host, port)
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(server.start())
        loop.run_forever()
    except KeyboardInterrupt:
        print("Controller stopping...")
    finally:
        loop.run_until_complete(server.close())

if __name__ == "__main__":
    main()
