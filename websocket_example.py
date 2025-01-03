import asyncio
import asyncpg
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
                    uid integer,
                    data integer[],
                    length integer
                );
            """
            )

            await connection.execute(
                f"""
                INSERT INTO datasets (uid, data, length)
                    VALUES (1, '{{1, 2, 3}}', 3);
            """
            )
            await connection.execute(
                f"""
                INSERT INTO datasets (uid, data, length)
                    VALUES (2, '{{100, 101, 102}}', 3);
            """
            )

    print("done")


@app.websocket("/notify/{uid:path}")
async def notify(uid: str, websocket: WebSocket):
    """
    Websocket endpoint to receive notifications about new data.

    Parameters
    ----------
    uid : str
        The uid of the dataset to receive notifications about.
    websocket : WebSocket
        The websocket connection.

    Returns
    -------
    None
    """

    await websocket.accept()
    uid = 1
    # Take a connection from the pool.
    async with app.pool.acquire() as connection:

        async def callback(conn, pid, channel, payload):
            await websocket.send_json({})

        await connection.add_listener(f"notifications_{uid}", callback)

        while True:
            await asyncio.sleep(1)


class Record(BaseModel):
    data: int


@app.put("/append/{uid}")
async def append(uid: str, record: Annotated[Record, Body(embed=True)]):
    """
    Add a new item to the dataset and notify listeners.

    Parameters
    ----------
    uid : str
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
                    UPDATE datasets SET data = array_append(data, {record.data}) WHERE uid={uid};
                    UPDATE datasets SET length = length + 1 WHERE uid={uid};
                """
            )

            # Create a notification on channel `notifications_{uid}` so that it can be picked
            # up by listeners of this channel.
            await connection.execute(
                f"NOTIFY notifications_{uid}, 'added data: {record.data}';"
            )
            # Create a notification on channel `notifications_all` so that generic listeners
            # can be notified of update.
            await connection.execute(
                f"NOTIFY notifications_all, 'added data: {record.data}';"
            )


@app.websocket("/stream/{uid}")
async def websocket_endpoint(uid: str, websocket: WebSocket, cursor: int = 0):
    """
    WebSocket endpoint to stream dataset records to the client.
    Parameters
    ----------
    uid : str
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
            uid, data, length = await connection.fetchrow(
                f"SELECT * FROM datasets WHERE uid={uid} LIMIT 1;"
            )
            print(f"server {uid = }, {data = }")
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

    async def inserter(uid):
        nonlocal ac
        await asyncio.sleep(2.0)
        for i in range(4):
            await ac.put(
                f"http://localhost/append/{uid}",
                content=json.dumps({"record": {"data": i}}),
            )
            print(f"client appended {uid = }, record {i}")
            await asyncio.sleep(1)

    async def notification_listener(uid):
        nonlocal ac
        async with aconnect_ws(f"http://localhost/notify/{uid}", ac) as ws:
            for i in range(3):
                message = await ws.receive_json()
                print(f"client received notification {uid = }, {message = }")
                await asyncio.sleep(1)

    async def stream_listener(uid):
        nonlocal ac
        async with aconnect_ws(f"http://localhost/stream/{uid}?cursor=1", ac) as ws:
            for i in range(3):
                message = await ws.receive_json()
                print(f"client received data {uid = }, {message=}")
                await asyncio.sleep(1)

    ac = AsyncClient(
        transport=ASGIWebSocketTransport(app=app), base_url="http://localhost"
    )
    async with asyncio.TaskGroup() as tg:
        tg.create_task(inserter(1))
        tg.create_task(inserter(2))
        tg.create_task(notification_listener(1))
        tg.create_task(notification_listener("all"))
        tg.create_task(stream_listener(2))


if __name__ == "__main__":
    asyncio.run(db_init())
    uvicorn.run(app)
