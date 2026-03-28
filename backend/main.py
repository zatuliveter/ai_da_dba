import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from config import validate_config
from routers.chat_files import router as chat_files_router
from routers.chats import router as chats_router
from routers.databases import router as databases_router
from static_files import mount_frontend
from store import init_db
from websocket_chat import router as websocket_router

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    validate_config()
    yield


app = FastAPI(title="AI da DBA", lifespan=lifespan)

app.include_router(databases_router)
app.include_router(chats_router)
app.include_router(chat_files_router)
app.include_router(websocket_router)

mount_frontend(app)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8888, reload=True)
