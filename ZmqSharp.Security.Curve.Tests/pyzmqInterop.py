"""CURVE interop peer driven by ZmqSharp's PyZmqInteropTests.

Binds a CURVE server PAIR socket, then echoes three single-frame messages
exactly as received. Prints one line per stage on stdout - "READY <endpoint>"
once the socket is bound and "PYZMQ-ECHO-DONE" after the exchange - for the
C# test to synchronize on; any payload mismatch prints "PYZMQ-ECHO-FAIL"
and exits non-zero.
"""

import argparse
import sys

import zmq

SERVER_SECRET = bytes.fromhex(
    "77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")

EXPECTED = [b"interop-1", b"interop-2", b"interop-3"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bind", required=True, help="tcp://127.0.0.1:<port>")
    args = parser.parse_args()

    with zmq.Context() as context, context.socket(zmq.PAIR) as socket:
        socket.curve_server = True
        socket.curve_secretkey = SERVER_SECRET
        socket.setsockopt(zmq.RCVTIMEO, 15000)
        socket.setsockopt(zmq.SNDTIMEO, 15000)
        socket.bind(args.bind)
        print(f"READY {args.bind}", flush=True)

        for expected in EXPECTED:
            actual = socket.recv()
            if actual != expected:
                print(f"PYZMQ-ECHO-FAIL {actual!r}", flush=True)
                return 1
            socket.send(expected)

        print("PYZMQ-ECHO-DONE", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
