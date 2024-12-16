from typing import Annotated
import uvicorn
import asyncio
import json
import pytest
import time
import threading
import psycopg2
import asyncpg

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from fastapi import FastAPI, Body, Depends
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel
from httpx import ASGITransport, AsyncClient

app = FastAPI()
app.pool = None

async def db_init():
    app.pool = await asyncpg.create_pool(user='postgres', password='secret',
                                  database='streaming-test-postgres', host='localhost')

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
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()

    # Take a connection from the pool.
    async with app.pool.acquire() as connection:
        # Open a transaction.
        async with connection.transaction():
            # Listen for notifications on channel `notrigger_test`.
            await connection.execute("LISTEN notrigger_test;")

            async def callback(conn, pid, channel, payload):
                await websocket.send_json({"notification": payload})
                print(payload)

            await connection.add_listener('notrigger_test', callback)
            await connection.execute('LISTEN notrigger_test')

            while True:
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
            await connection.execute(f"NOTIFY notrigger_test, 'added data: {record.data}';")

            val = await connection.fetchval("SELECT length FROM datasets WHERE uid=1 LIMIT 1;")
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


# @pytest.fixture(scope="session")
# async def api_fixture():
#     async with TestClient(app) as client:
#         await db_init()
#         yield client
#client = TestClient(app)
#asyncio.run(db_init())


@pytest.mark.anyio
async def test_threaded():

    def inserter():
        for i in range(8):
            print("calling api_fixture.put")
            #client.put("/append", data=json.dumps({"record": {"data": i}}))
            time.sleep(0.5)

    t = threading.Thread(target=inserter)
    t.start()

    await db_init()

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://localhost"
    ) as ac:
        response = await ac.get("/")

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

    t.join()


if __name__ == "__main__":

    #asyncio.run(db_init())

    uvicorn.run(app)
