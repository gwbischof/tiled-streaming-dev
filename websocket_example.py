from typing import Annotated
from urllib import request
import uvicorn
import asyncio
import json
import pytest
import time
import threading
import psycopg2

from fastapi import FastAPI, Body
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel
from websockets.sync.client import connect

app = FastAPI()

conn = psycopg2.connect(
    dbname="streaming-test-postgres",
    user="postgres",
    host="localhost",
    password="secret",
)
cur = conn.cursor()

def db_init():
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
    await websocket.accept()
    cur.execute('SELECT * FROM datasets LIMIT 1;')
    uid, data, length = cur.fetchone()
    old_length = length
    while True:
        print(f"{length = }")
        print(f"{data = }")
        cur.execute('SELECT * FROM datasets WHERE uid=1 LIMIT 1;')
        _, data, length = cur.fetchone()
        if length > old_length:
            await websocket.send_json({"message": "new data", "length": length})
            old_length = length
        await asyncio.sleep(0.5)


class Record(BaseModel):
    data: int


@app.put("/append")
async def insert(record: Annotated[Record, Body(embed=True)]):
    cur.execute(
        f"""
            UPDATE datasets SET data = array_append(data, {record.data}) WHERE uid=1;
            UPDATE datasets SET length = length + 1 WHERE uid=1;
        """
    )

    cur.execute('SELECT length FROM datasets WHERE uid=1 LIMIT 1')

    length = cur.fetchone()
    return {"length": length}


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket, cursor: int = 0):
    # How do you know when a dataset is completed?
    # For this test we just send a None when the dataset is completed.
    await websocket.accept()
    while True:
        cur.execute('SELECT * FROM datasets WHERE uid=1 LIMIT 1;')
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
            api_fixture.put('/append', data=json.dumps({"record": {"data": i}}))
            time.sleep(0.5)

    t = threading.Thread(target=inserter)
    t.start()

    print("Beginning")
    # Wait for a notification.
    with api_fixture.websocket_connect("/notify") as notify_websocket:
        notification = notify_websocket.receive_json()
        print("notification", notification)

    print("Start stream")
    # Read the new data.
    with api_fixture.websocket_connect(f"/stream?cursor=2") as websocket:
        for i in range(5):
            response = websocket.receive_json()
            print("websocket", response)
    
    t.join()


def test_postgres_connectivity():
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

    cur.execute(
        f"""
        UPDATE datasets SET data = array_append(data, 99) WHERE uid=1;
        UPDATE datasets SET length = length + 1 WHERE uid=1;
    """
    )

    cur.execute('SELECT * FROM datasets LIMIT 1')
    print(cur.fetchone())


if __name__ == "__main__":
    uvicorn.run(app)
