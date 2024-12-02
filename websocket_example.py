import contextlib
import subprocess
import sys
from typing import Annotated
from urllib import request
import uvicorn
import asyncio
import json
import pytest
import random
import time
import requests
import threading
import psycopg2
import uuid

from fastapi import FastAPI, Body
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel
from websockets.sync.client import connect

app = FastAPI()

# Array to hold data.
data = []


@app.websocket("/notify")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    old_cursor = cursor = len(data)
    while True:
        print("cursor", cursor)
        print("data", data)
        cursor = len(data)
        if cursor > old_cursor:
            await websocket.send_json({"message": "new data", "cursor": cursor})
            old_cursor = cursor
        await asyncio.sleep(1)


class Record(BaseModel):
    data: int


@app.put("/append")
async def insert(record: Annotated[Record, Body(embed=True)]):
    data.append(record.data)
    return {"uid": len(data)}


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket, cursor: int = 0):
    await websocket.accept()
    while True:
        if cursor < len(data):
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
        yield client

@contextlib.contextmanager
def api():
    with TestClient(app) as client:
        yield client


# def test_notify(api_fixture):
#     with api_fixture.websocket_connect("/notify") as websocket:
#         for i in range(10):
#             data = websocket.receive_json()
#             print(data)


def test_insert(api_fixture):
    for i in range(5):
        response = api_fixture.put(
            "/append",
            json={"record": {"data": i}},
        )
        # assert response.status_code == 200
        print(response.json())
        print(data)


def inserter():
    i = 0
    while True:
        requests.put(
            "http://127.0.0.1:8000/append", data=json.dumps({"record": {"data": i}})
        )
        time.sleep(0.5)
        i += 1


@contextlib.contextmanager
def inserter_process():
    try:
        ps = subprocess.Popen(
            [
                sys.executable,
                "-c",
                f"from websocket_example import inserter; inserter()",
            ]
        )
        time.sleep(0.5)
        yield ps
    finally:
        ps.terminate()


@contextlib.contextmanager
def api_process():
    try:
        ps = subprocess.Popen(
            [
                sys.executable,
                "-c",
                f"from websocket_example import app; import uvicorn; uvicorn.run(app)",
            ]
        )
        time.sleep(0.5)
        yield ps
    finally:
        ps.terminate()


def test_multiprocess():
    with api_process():
        with inserter_process():
            with connect("ws://localhost:8000/stream") as websocket:
                websocket.send("Hello world!")
                message = websocket.recv()
                print(message)
            with api_fixture.websocket_connect("/stream") as websocket:
                with inserter_process():
                    while True:
                        data = websocket.receive_json()
                        print("websocket", data)


def test_threaded(api_fixture):
    def inserter_thread():
        for i in range(10):
            data.append(i)
            time.sleep(1)
        data.append(None)

    t = threading.Thread(target=inserter_thread)
    t.start()

    cursor = 2

    print("Beginning")
    # Wait for a notification.
    with api_fixture.websocket_connect("/notify") as notify_websocket:
        notification = notify_websocket.receive_json()
        print("notification", notification)

    print("Start stream")
    # Read the new data.
    with api_fixture.websocket_connect(f"/stream?cursor={cursor}") as websocket:
        while True:
            response = websocket.receive_json()
            if response['record'] is None:
                break
            print("websocket", response)


def test_postgres_connectivity():
    conn = psycopg2.connect(dbname='streaming-test-postgres', user='postgres', host='localhost', password='secret')
    cur = conn.cursor()
    # cur.execute('SELECT 1')
    # assert cur.fetchone()[0] == 1

    cur.execute('''
        CREATE TABLE datasets (
            uid uuid,
            data integer[],
            length integer
        );
    ''')
    cur.execute(f'''
        INSERT INTO datasets (uid, data, length)
            VALUES ('{uuid.uuid4()}', '{{1, 2, 3}}', 3);
    ''')
    cur.execute('SELECT * FROM datasets LIMIT 1')
    print(cur.fetchone())

#{'timestamp': "time", 'uid': 123123, length: 2 'data': [1, 2]}
# TODO:
# - update the inserter to append to the data list in datasets table
# - update notify and stream endpoints to poll from postgress and stream

if __name__ == "__main__":
    uvicorn.run(app)
