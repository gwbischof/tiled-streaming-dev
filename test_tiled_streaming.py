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


@pytest.mark.asyncio
async def test_async():
    """
    Asynchronous test function to test all of the components of streaming data together.
    """

    async def inserter(path):
        nonlocal ac
        await asyncio.sleep(2.0)
        for i in range(4):
            await ac.put(
                    f"http://localhost:8000/append/{path}",
                content=json.dumps({"record": {"data": i}}),
            )
            print(f"client appended {path = }, record {i}")
            await asyncio.sleep(1)

    async def notification_listener(path):
        nonlocal ac
        subprotocols = ["v1"]
        async with aconnect_ws(
                f"http://localhost:8000/notify/{path}", ac, subprotocols=subprotocols,
            headers={'accept': 'application/json'}
        ) as ws:
            print("CLIENT HEADERS", ws.response.headers)
            for i in range(3):
                message = await ws.receive_text()
                print(f"client received notification {path = }, {message = }")
                await asyncio.sleep(1)

    ac = AsyncClient(
            transport=ASGIWebSocketTransport(app=app), base_url="http://localhost:8000"
    )
    async with asyncio.TaskGroup() as tg:
        # Insert into dataset 1.
        tg.create_task(inserter("root/1"))
        tg.create_task(notification_listener("root/1"))


if __name__ == "__main__":
    asyncio.run(db_init())
    uvicorn.run(app)
