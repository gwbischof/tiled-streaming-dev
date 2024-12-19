from typing import Annotated
import uvicorn
import asyncio
import json
import pytest
import time
import threading
import psycopg2
import asyncpg
import httpx

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from fastapi import FastAPI, Body, Depends
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel
from httpx import ASGITransport, AsyncClient
from httpx_ws import aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport


app = FastAPI()
app.pool = None


async def db_init():
    app.pool = await asyncpg.create_pool(
        user="postgres",
        password="secret",
        database="streaming-test-postgres",
        host="localhost",
    )

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:
        # Open a transaction.
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

    print("done")


@app.websocket("/notify")
async def notify(websocket: WebSocket):
    await websocket.accept()

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:
        # Open a transaction.
        async with connection.transaction():
            # Listen for notifications on channel `notrigger_test`.
            await connection.execute("LISTEN notrigger_test;")

            # TODO: figure out why callback is not getting triggered
            # async def callback(conn, pid, channel, payload):
            #     print("/notify4")
            await asyncio.sleep(2.0)
            await websocket.send_json({"notification": 12341234})

            # print(payload)

            await connection.add_listener("notrigger_test", callback)
            await connection.execute("LISTEN notrigger_test")

            # while True:
            await connection.wait()  # wait for notification


class Record(BaseModel):
    data: int


@app.put("/append")
async def append(record: Annotated[Record, Body(embed=True)]):

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:
        # Open a transaction.
        async with connection.transaction():

            await connection.execute(
                f"""
                    UPDATE datasets SET data = array_append(data, {record.data}) WHERE uid=1;
                    UPDATE datasets SET length = length + 1 WHERE uid=1;
                """
            )

            # Create a notification on channel `notrigger_test` so that can be picked
            # up by listeners of this channel.
            await connection.execute(
                f"NOTIFY notrigger_test, 'added data: {record.data}';"
            )

            val = await connection.fetchval(
                "SELECT length FROM datasets WHERE uid=1 LIMIT 1;"
            )
            print(f"appended {record = }")
            return {"length": val}


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket, cursor: int = 0):
    # How do you know when a dataset is completed?
    await websocket.accept()
    while True:
        await app.conn.execute("SELECT * FROM datasets WHERE uid=1 LIMIT 1;")
        _, data, length = cur.fetchone()
        if cursor < length:
            await websocket.send_json({"record": data[cursor]})
            cursor += 1
        else:
            await asyncio.sleep(1)


@app.get("/")
async def root():
    return {"message": "Testing"}


@pytest.mark.asyncio
async def test_async():

    await db_init()

    async def inserter():
        print("INSERTER")
        nonlocal ac
        for i in range(8):
            await ac.put(
                "http://localhost/append", content=json.dumps({"record": {"data": i}})
            )
            await asyncio.sleep(1)

    async def notifier():
        print("NOTIFIER")
        nonlocal ac
        # TODO: need to figure out how to make this ws client.
        async with aconnect_ws("http://localhost/notify", ac) as ws:
            # breakpoint()
            message = await ws.receive_json()
            print("NOTIFIER -> aconnect_ws")
            print("websocket", message)
            # for i in range(8):
            #     print("NOTIFIER -> aconnect_ws")
            #     await asyncio.sleep(1)

    ac = AsyncClient(
        transport=ASGIWebSocketTransport(app=app), base_url="http://localhost"
    )
    # async with asyncio.TaskGroup() as tg:
    #     insert_task = tg.create_task(inserter())
    #     notify_task = tg.create_task(notifier())

    inserter_task = asyncio.create_task(inserter())
    notifier_task = asyncio.create_task(notifier())

    _ = await asyncio.wait([inserter_task, notifier_task])

    # Wait for a notification.
    # with client.websocket_connect("/notify") as notify_websocket:
    #     notification = notify_websocket.receive_json()
    #     print("notification", notification)

    # print("Start stream")
    # # Read the new data.
    # with api_fixture.websocket_connect(f"/stream?cursor=2") as websocket:
    #     for i in range(5):
    #         response = websocket.receive_json()
    #         print("websocket", response)


if __name__ == "__main__":
    asyncio.run(db_init())
    uvicorn.run(app)
