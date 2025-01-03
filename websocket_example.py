import asyncio
import asyncpg
import hashlib
import json
import pytest
import uvicorn

from fastapi import FastAPI, Body, WebSocket
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
                    uid integer,
                    data integer[],
                    length integer
                );
            """
            )

            await connection.execute(
                f"""
                INSERT INTO datasets (path, uid, data, length)
                    VALUES ('root', 1, '{{1, 2, 3}}', 3);
            """
            )
            await connection.execute(
                f"""
                INSERT INTO datasets (path, uid, data, length)
                    VALUES ('root', 2, '{{100, 101, 102}}', 3);
            """
            )

    print("done")


def path_hash(path: str) -> str:
    """
    Generate a hash from a path.
    """
    # ensure hash is a valid postgres identifier by prefixing with a string
    # valid identifiers start with a letter or "_" and contain only letters, numbers, and "_"
    # and are at most 63 characters long
    return "md5" + hashlib.md5(path.encode()).hexdigest()


@app.websocket("/notify/{path:path}")
async def notify(path: str, websocket: WebSocket):
    """
    Websocket endpoint to receive notifications about new data.

    Parameters
    ----------
    path : str
        The uid of the dataset to receive notifications about.
    websocket : WebSocket
        The websocket connection.

    Returns
    -------
    None
    """

    await websocket.accept()

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:

        async def callback(conn, pid, channel, payload):
            await websocket.send_json({})

        await connection.add_listener(
            f"n_{path_hash(path)}", callback
        )

        while True:
            await asyncio.sleep(1)


class Record(BaseModel):
    data: int


@app.put("/append/{path:path}/{uid}")
async def append(path: str, uid: int, record: Annotated[Record, Body(embed=True)]):
    """
    Add a new item to the dataset and notify listeners.

    Parameters
    ----------
    path : str
        The unique identifier of the dataset to which the new item will be appended.
    record : Annotated[Record, Body(embed=True)]
        The record containing the data to be appended to the dataset.

    Returns
    -------
    None
    """

    async with app.pool.acquire() as connection:
        async with connection.transaction():
            # Append new value to data and increment the length.
            await connection.execute(
                f"""
                    UPDATE datasets SET data = array_append(data, {record.data}) WHERE uid={uid} AND path='{path}';
                    UPDATE datasets SET length = length + 1 WHERE uid={uid} AND path='{path}';
                """
            )

            # Create a notification on the dataset and parent channels.            
            split_path = path.split('/') + [str(uid)]
            for i in range(1, len(split_path) +1):
                sub_path = '/'.join(split_path[0:i])
                await connection.execute(
                    f"NOTIFY n_{path_hash(sub_path)}, 'added data: {record.data}';"
                )


@app.websocket("/stream/{path:path}/{uid}")
async def websocket_endpoint(path: str, uid: str,  websocket: WebSocket, cursor: int = 0):
    """
    WebSocket endpoint to stream dataset records to the client.
    Parameters
    ----------
    path : str
        Unique identifier for the dataset.
    websocket : WebSocket
        WebSocket connection instance.
    cursor : int, optional
        Starting position in the dataset (default is 0).
    Returns
    -------
    None
    """

    # How do you know when a dataset is completed?
    await websocket.accept()
    while True:
        async with app.pool.acquire() as connection:
            path, uid, data, length = await connection.fetchrow(
                f"SELECT * FROM datasets WHERE uid={uid} AND path='{path}' LIMIT 1;"
            )
            print(f"server {path = }, {data = }")
        if cursor < length:
            await websocket.send_json({"record": data[cursor]})
            cursor += 1
        else:
            await asyncio.sleep(1)


@pytest.mark.asyncio
async def test_async():
    """
    Asynchronous test function to test all of the components of streaming data together.

    FastAPI endpoints:
    /notify/{uid} - Websocket endpoint to receive notifications about new data.
    /notify/all - Websocket endpoint to receive notifications about new data for all datasets.
    /stream/{uid} - Websocket endpoint to stream dataset records to the client.

    This test initializes a database connection and creates multiple asynchronous tasks to:
    1. Insert data into the server.
    2. Listen for notifications from the server.
    3. Listen for data streams from the server.
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
        async with aconnect_ws(f"http://localhost/notify/{path}", ac) as ws:
            for i in range(3):
                message = await ws.receive_json()
                print(f"client received notification {path = }, {message = }")
                await asyncio.sleep(1)

    async def stream_listener(path):
        nonlocal ac
        async with aconnect_ws(f"http://localhost/stream/{path}?cursor=1", ac) as ws:
            for i in range(3):
                message = await ws.receive_json()
                print(f"client received data {path = }, {message=}")
                await asyncio.sleep(1)

    ac = AsyncClient(
        transport=ASGIWebSocketTransport(app=app), base_url="http://localhost"
    )
    async with asyncio.TaskGroup() as tg:
        tg.create_task(inserter("root/1"))  # Insert into dataset 1.
        tg.create_task(inserter("root/2"))  # Insert into dataset 2.
        tg.create_task(notification_listener("root/1"))  # Get notifications for dataset 1.
        tg.create_task(notification_listener("root"))  # Get notifications for the parent of dataset 1.
        tg.create_task(stream_listener("root/1"))  # Get dataset 1 data stream.
        tg.create_task(stream_listener("root/2"))  # Get dataset 2 data stream.


if __name__ == "__main__":
    asyncio.run(db_init())
    uvicorn.run(app)
