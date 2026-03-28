import os
from fastapi.staticfiles import StaticFiles
from backend.config import ROOT_DIR

FRONTEND_DIR = ROOT_DIR / "frontend"


class NoCacheStaticFiles(StaticFiles):
    """StaticFiles that sends Cache-Control: no-cache for .js and .css (when DEVELOPMENT is set)."""

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if path.endswith((".js", ".css")) and os.environ.get("DEVELOPMENT", "").lower() in ("1", "true", "yes"):
            async def send_with_no_cache(message):
                if message.get("type") == "http.response.start":
                    headers = list(message.get("headers", []))
                    headers.append((b"cache-control", b"no-cache"))
                    message = {**message, "headers": headers}
                await send(message)
            await super().__call__(scope, receive, send_with_no_cache)
        else:
            await super().__call__(scope, receive, send)


def mount_frontend(app):
    app.mount("/", NoCacheStaticFiles(directory=str(FRONTEND_DIR), html=True), name="frontend")
