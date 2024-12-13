from typing import Annotated
import uvicorn
import asyncio
import json
import pytest
import time
import threading
import psycopg2

from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from fastapi import FastAPI, Body
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel

app = FastAPI()

conn = psycopg2.connect(
    dbname="streaming-test-postgres",
    user="postgres",
    host="localhost",
    password="secret",
)

# Autocommit after each command.
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)


def db_init():
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS datasets;")

    cur.execute(
        """
        CREATE TABLE datasets (
            uid integer,
            data integer[],
            length integer
        );
    """
    )

    cur.execute(
        f"""
        INSERT INTO datasets (uid, data, length)
            VALUES (1, '{{1, 2, 3}}', 3);
    """
    )

    return cur


@app.websocket("/notify")
async def websocket_endpoint(websocket: WebSocket):
    cur = conn.cursor()
    await websocket.accept()

    # Listen for notifications on channel `notrigger_test`.
    cur.execute("LISTEN notrigger_test;")

    while True:
        # TODO: Need to figure out how to make poll not block indefinitely.
        await asyncio.sleep(0.5)
        conn.poll()
        for notify in conn.notifies:
            print(notify.payload)
        conn.notifies.clear()


class Record(BaseModel):
    data: int


@app.put("/append")
async def append(record: Annotated[Record, Body(embed=True)]):
    cur = conn.cursor()
    cur.execute(
        f"""
            UPDATE datasets SET data = array_append(data, {record.data}) WHERE uid=1;
            UPDATE datasets SET length = length + 1 WHERE uid=1;
        """
    )

    # Create a notification on channel `notrigger_test` so that can be picked
    # up by listeners of this channel.
    cur.execute(f"NOTIFY notrigger_test, 'added data: {record.data}';")

    cur.execute("SELECT length FROM datasets WHERE uid=1 LIMIT 1;")
    length = cur.fetchone()

    print(f"appended {record = }")
    return {"length": length}


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket, cursor: int = 0):
    # How do you know when a dataset is completed?
    cur = conn.cursor()
    await websocket.accept()
    while True:
        cur.execute("SELECT * FROM datasets WHERE uid=1 LIMIT 1;")
        _, data, length = cur.fetchone()
        if cursor < length:
            await websocket.send_json({"record": data[cursor]})
            cursor += 1
        else:
            await asyncio.sleep(1)


@app.get("/")
async def root():
    return {"message": "Testing"}


@pytest.fixture(scope="session")
def api_fixture():
    with TestClient(app) as client:
        db_init()
        yield client


def test_threaded(api_fixture):

    def inserter():
        for i in range(8):
            print("calling api_fixture.put")
            api_fixture.put("/append", data=json.dumps({"record": {"data": i}}))
            time.sleep(0.5)

    t = threading.Thread(target=inserter)
    t.start()

    # Wait for a notification.
    with api_fixture.websocket_connect("/notify") as notify_websocket:
        notification = notify_websocket.receive_json()
        print("notification", notification)

    t.join()


if __name__ == "__main__":
    uvicorn.run(app)
