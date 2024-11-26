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

from fastapi import FastAPI, Body
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel
from websockets.sync.client import connect

app = FastAPI()

# Array to hold data.
data = []



# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     i = 0
#     while True:
#         data = {'data': i}
#         output = json.dumps(data)
#         await websocket.send_text(output)
#         await asyncio.sleep(1)
#         i += 1


# @app.websocket("/ws_readwrite")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     i = 0
#     while True:
#         response = await websocket.receive_json()
#         data = {'data': response, 'index': i}
#         await websocket.send_json(data)
#         await asyncio.sleep(1)
#         i += 1


# @app.websocket("/notify")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     i = 0
#     while True:
#         num = random.randint(0, 4)
#         if num == 2:
#             await websocket.send_json({"message": "new data", "index": i})
#             await asyncio.sleep(1)
#         i+=1


class Record(BaseModel):
    data: int

@app.put("/insert")
async def insert(record: Annotated[Record, Body(embed=True)]):
    data.append(record.data)
    return {'uid': len(data)}

# @app.put("/insert/{record}")
# async def insert(record: int):
#     data.append(record)
#     return {'uid': len(data)}


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket, cursor: int = 0):
    await websocket.accept()
    while True:
        if cursor < len(data):
            await websocket.send_json({'record': data[cursor]})
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


# def test_read(api_fixture):
#     with api_fixture.websocket_connect("/ws") as websocket:
#         for i in range(10):
#             data = websocket.receive_json()
#             print(data)


# def test_readwrite(api_fixture):
#     with api_fixture.websocket_connect("/ws_readwrite") as websocket:
#         for i in range(10):
#             message = {"message": i}
#             websocket.send_json(message)
#             data = websocket.receive_json()
#             print(data)

# def test_notify(api_fixture):
#     with api_fixture.websocket_connect("/notify") as websocket:
#         for i in range(10):
#             data = websocket.receive_json()
#             print(data)

def test_insert(api_fixture):
    for i in range(5):
        response = api_fixture.put(
            "/insert",
            json={'record': {'data': i}},
        )
        #assert response.status_code == 200
        print(response.json())
        print(data)

def inserter():
    i = 0
    while True:
        requests.put("http://127.0.0.1:8000/insert", data=json.dumps({'record': {'data': i}}))
        time.sleep(0.5)
        i+=1
    
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

def test_asyncronous():
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

def test_syncronous(api_fixture):
    test_insert(api_fixture)
    with api_fixture.websocket_connect("/stream?cursor=2") as websocket:
        for i in range(3):
            data = websocket.receive_json()
            print("websocket", data)
    # with api_fixture.websocket_connect("/stream", json={'cursor': 0}) as websocket:
    #     for i in range(5):
    #         response = api_fixture.put(
    #             "/insert",
    #             json={'record': {'data': i}},
    #         )
    #         print(response.json())
    #         data = websocket.receive_json()
    #         print(data)

def inserter_thread():
	for i in range(10):
		data.append(i)
		time.sleep(1)

def test_async(api_fixture):
    t = threading.Thread(target=inserter_thread)
    t.start()
    with api_fixture.websocket_connect("/stream?cursor=2") as websocket:
        while True:
            data = websocket.receive_json()
            print("websocket", data)

if __name__ == "__main__":
    uvicorn.run(app)
