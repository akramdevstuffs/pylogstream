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
    parser.add_argument("--zk-hosts", dest="zk_hosts", help="ZooKeeper hosts (e.g. 127.0.0.1:2181)")
    parser.add_argument("--controller-id", dest="controller_id", help="Unique controller ID")
    args = parser.parse_args()

    host = "127.0.0.1"
    port = 9093
    zk_hosts = None
    controller_id = ""

    try:
        cfg = load_config(args.config)
        host = cfg.controller.host
        port = cfg.controller.port
        zk_hosts = cfg.controller.zk_hosts or None
        controller_id = cfg.controller.controller_id
    except Exception as e:
        print(f"Warning: Could not load config file {args.config} ({e}). Using defaults.")

    if args.host:
        host = args.host
    if args.port:
        port = args.port
    if args.zk_hosts:
        zk_hosts = args.zk_hosts
    if args.controller_id:
        controller_id = args.controller_id

    server = ControllerServer(
        host, port,
        zk_hosts=zk_hosts,
        controller_id=controller_id,
    )

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
