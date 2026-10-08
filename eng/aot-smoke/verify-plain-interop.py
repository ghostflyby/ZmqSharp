"""Verify all PLAIN roles/topologies against pyzmq's libzmq reference.

Usage: python3 eng/aot-smoke/verify-plain-interop.py /path/to/published/Smoke
Requires pyzmq; the normal .NET tests and AOT smoke do not depend on Python.
"""
import asyncio
import socket
import sys

import zmq
import zmq.asyncio
from zmq.auth.asyncio import AsyncioAuthenticator


async def exchange(binary, context, local_server, local_binds):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    endpoint = f"tcp://127.0.0.1:{port}"
    reference = context.socket(zmq.PAIR)
    reference.linger = 0
    if local_server:
        reference.plain_username = b"alice"
        reference.plain_password = b"secret"
    else:
        reference.plain_server = True
        reference.zap_domain = b"*"
    if not local_binds:
        reference.bind(endpoint)
    process = await asyncio.create_subprocess_exec(
        binary, "plain-peer", "server" if local_server else "client",
        "bind" if local_binds else "connect", endpoint,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        if local_binds:
            assert await asyncio.wait_for(process.stdout.readline(), 15) == b"BOUND\n"
            reference.connect(endpoint)
        await reference.send(b"reference")
        assert await asyncio.wait_for(reference.recv(), 15) == b"verified"
        out, err = await asyncio.wait_for(process.communicate(), 15)
        assert process.returncode == 0, (out, err)
        print(f"PASS local={'server' if local_server else 'client'}, {'bind' if local_binds else 'connect'}")
    except Exception as error:
        if process.returncode is None:
            process.kill()
        out, err = await process.communicate()
        raise RuntimeError(f"role={local_server}, bind={local_binds}, exit={process.returncode}, out={out!r}, err={err!r}") from error
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
        reference.close()


async def main():
    context = zmq.asyncio.Context()
    auth = AsyncioAuthenticator(context)
    auth.start()
    auth.configure_plain(domain="*", passwords={"alice": "secret"})
    try:
        for server in (False, True):
            for binds in (False, True):
                await exchange(sys.argv[1], context, server, binds)
    finally:
        auth.stop()
        context.term()


asyncio.run(main())
