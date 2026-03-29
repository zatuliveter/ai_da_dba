import logging.config
from copy import deepcopy
from contextlib import asynccontextmanager

from fastapi import FastAPI
from uvicorn.config import LOGGING_CONFIG

_log_config = deepcopy(LOGGING_CONFIG)
_log_config["formatters"]["default"]["fmt"] = (
    "%(levelprefix)s %(name)s: %(message)s"
)
_log_config["loggers"]["backend"] = {
    "handlers": ["default"],
    "level": "INFO",
    "propagate": False,
}
logging.config.dictConfig(_log_config)

from backend.config import validate_config
from backend.web.routers.chat_files import router as chat_files_router
from backend.web.routers.chats import router as chats_router
from backend.web.routers.databases import router as databases_router
from backend.web.frontend_mount import mount_frontend
from backend.ai.store import init_db
from backend.web.websocket_chat import router as websocket_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_config()
    init_db()
    yield


app = FastAPI(title="AI da DBA", lifespan=lifespan)

app.include_router(databases_router)
app.include_router(chats_router)
app.include_router(chat_files_router)
app.include_router(websocket_router)

mount_frontend(app)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8888,
        reload=True,
        log_config=_log_config,
    )
