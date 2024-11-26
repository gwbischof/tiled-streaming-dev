import contextlib
from typing import Annotated
import uvicorn
import asyncio
import json
import pytest
import random

from fastapi import FastAPI, Body
from fastapi import WebSocket
from fastapi.testclient import TestClient
from pydantic import BaseModel

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
    data.append(record)
    return {'uid': len(data)}

# @app.put("/insert/{record}")
# async def insert(record: int):
#     data.append(record)
#     return {'uid': len(data)}


@app.websocket("/stream")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    i = 0
    while True:
        pass


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
            json={'record': {'data': 1}},
        )
        #assert response.status_code == 200
        print(response.json())
        print(data)


if __name__ == "__main__":
    uvicorn.run(app)
