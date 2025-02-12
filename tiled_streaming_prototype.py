import asyncio
import asyncpg
import hashlib
import json
import numpy as np
import pytest
import uvicorn

from fastapi import FastAPI, Body, WebSocket, status
from fastapi.exceptions import WebSocketException
from httpx import AsyncClient
from httpx_ws import aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport
from pydantic import BaseModel
from typing import Annotated


app = FastAPI()
app.pool = None


async def db_init():
    """
    Setup the database.
    """
    app.pool = await asyncpg.create_pool(
        user="postgres",
        password="secret",
        database="streaming-test-postgres",
        host="localhost",
    )

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:
        # Open a transaction, this makes these operations atomic.
        async with connection.transaction():

            await connection.execute("DROP TABLE IF EXISTS datasets;")

            await connection.execute(
                """
                CREATE TABLE datasets (
                    path text,
                    uid text,
                    data integer[],
                    length integer,
                    UNIQUE (path, uid)
                );
                """
            )


def path_hash(path: str) -> str:
    """
    Generate a hash from a path.
    """
    # ensure hash is a valid postgres identifier by prefixing with a string
    # valid identifiers start with a letter or "_" and contain only letters, numbers, and "_"
    # and are at most 63 characters long
    return "shake256_" + hashlib.shake_256(path.encode()).hexdigest(20)


async def websocket_accept(websocket, mimetypes, subprotocols):

    requested_subprotocols = websocket.headers.get("sec-websocket-protocol").split(", ")
    requested_mimetypes = websocket.headers.get("accept", "*/*").split(", ")
    print("HEADER", websocket.headers)
    if intersect := [item for item in requested_mimetypes if item in mimetypes]:
        mimetype = intersect[0]
    else:
        await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
        return

    if intersect := [ item for item in requested_subprotocols if item in subprotocols]:
        subprotocol = intersect[0]
    else:
        await websocket.close(code=status.WS_1002_PROTOCOL_ERROR)
        return

    await websocket.accept(subprotocol=subprotocol, headers=[('content-type', mimetype)])
    return mimetype, subprotocol
    


@app.websocket("/notify/{path:path}")
async def notify(path: str, websocket: WebSocket):
    """
    Websocket endpoint to receive notifications about new data.

    Parameters
    ----------
    path : str
        The catalog path.
    websocket : WebSocket
        The websocket connection.
    """
    subprotocols = ["v1"]
    mimetypes = ["*/*", "application/json"]

    mimetype, subprotocol = await websocket_accept(websocket, mimetypes, subprotocols)

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:

        async def callback(conn, pid, channel, payload):
            if subprotocol == "v1":
                await websocket.send_json({})
            elif subprotocol == "v2":
                await websocket.send_json({"version": "v2"})

        await connection.add_listener(f"{path_hash(path)}", callback)

        while True:
            await asyncio.sleep(1)


class Record(BaseModel):
    data: int


@app.put("/append/{path:path}/{uid}")
async def append(path: str, uid: str, record: Annotated[Record, Body(embed=True)]):
    """
    Add a new item to the dataset and notify listeners.

    Parameters
    ----------
    uid: str
        The unique indentifier for the dataset.
    path : str
        The catalog path.
    record : Annotated[Record, Body(embed=True)]
        The record containing the data to be appended to the dataset.
    """

    async with app.pool.acquire() as connection:
        async with connection.transaction():
            # Append new value to data or create new record and increment the length.
            await connection.execute(
                f"""
                    INSERT INTO datasets (path, uid, data, length)
                    VALUES ('{path}', '{uid}', '{{{record.data}}}', 1)
                    ON CONFLICT (path, uid) DO UPDATE
                    SET data = array_append(datasets.data, {record.data}),
                        length = datasets.length + 1;
                """
            )

            # Create a notification on the dataset and parent channels.
            split_path = path.split("/") + [str(uid)]
            for i in range(1, len(split_path) + 1):
                sub_path = "/".join(split_path[0:i])
                await connection.execute(
                    f"NOTIFY {path_hash(sub_path)}, 'added data: {record.data}';"
                )


@app.websocket("/stream/{path:path}/{uid}")
async def websocket_endpoint(
    path: str, uid: str, websocket: WebSocket, cursor: int | None = None
):
    """
    WebSocket endpoint to stream dataset records to the client.

    Parameters
    ----------
    uid : str
        unique indentifier for the dataset.
    path : str
        catalog path.
    websocket : WebSocket
        WebSocket connection instance.
    cursor : int, optional
        Starting position in the dataset (default is 0).
    """

    # How do you know when a dataset is completed?
    subprotocols = ["v1"]
    mimetypes = ["*/*", "application/json"]
    mimetype, subprotocol = await websocket_accept(websocket, mimetypes, subprotocols)

    while True:
        async with app.pool.acquire() as connection:
            result = await connection.fetchrow(
                f"SELECT * FROM datasets WHERE uid='{uid}' AND path='{path}' LIMIT 1;"
            )
            if result is not None:
                path, uid, data, length = result
                if cursor is None:
                    cursor = length
                print(f"server {path = }, {data = }")
                while cursor < length:
                    if mimetype == "application/json":
                        await websocket.send_json({"record": data[cursor]})
                    elif mimetype == "application/octet-stream":
                        await websocket.send_bytes(np.array(data[cursor]).tobytes())
                    elif mimetype == "image/tiff":
                        with open(f"image.tiff", "rb") as tiff:
                            await websocket.send_bytes(tiff.read())
                    else:
                        raise WebSocketException(f"Invalid subprotocol: {subprotocols}")
                    cursor += 1
            await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_async():
    """
    Asynchronous test function to test all of the components of streaming data together.
    """

    await db_init()

    async def inserter(path):
        nonlocal ac
        await asyncio.sleep(2.0)
        for i in range(4):
            await ac.put(
                f"http://localhost/append/{path}",
                content=json.dumps({"record": {"data": i}}),
            )
            print(f"client appended {path = }, record {i}")
            await asyncio.sleep(1)

    async def notification_listener(path):
        nonlocal ac
        subprotocols = ["v1"]
        async with aconnect_ws(
            f"http://localhost/notify/{path}", ac, subprotocols=subprotocols,
            headers={'accept': 'application/json'}
        ) as ws:
            print("CLIENT HEADERS", ws.response.headers)
            for i in range(3):
                message = await ws.receive_text()
                print(f"client received notification {path = }, {message = }")
                await asyncio.sleep(1)

    async def stream_listener(path):
        nonlocal ac
        subprotocols = ["application/octet-stream"]
        
        async with aconnect_ws(
            f"http://localhost/stream/{path}?cursor=1", ac, 
            subprotocols=subprotocols,
        ) as ws:
            for i in range(3):
                message = await ws.receive_bytes()
                data = np.frombuffer(message, dtype=np.int64)
                print(f"client received data {path = }, {data=}")
                await asyncio.sleep(1)

    ac = AsyncClient(
        transport=ASGIWebSocketTransport(app=app), base_url="http://localhost"
    )
    async with asyncio.TaskGroup() as tg:
        # Insert into dataset 1.
        tg.create_task(inserter("root/1"))
        # Insert into dataset 2.
        tg.create_task(inserter("root/2"))
        # Get notifications for dataset 1.
        tg.create_task(notification_listener("root/1"))
        # Get notifications for the parent of dataset 1.
        tg.create_task(notification_listener("root"))
        # Get dataset 1 data stream.
        tg.create_task(stream_listener("root/1"))
        # Get dataset 2 data stream.
        tg.create_task(stream_listener("root/2"))


if __name__ == "__main__":
    asyncio.run(db_init())
    uvicorn.run(app)
