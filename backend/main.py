import json
import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from config import llm_client, LLM_MODEL
from db import list_databases
from prompts import SYSTEM_PROMPT
from tools import TOOL_DEFINITIONS, dispatch_tool

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("agent")

app = FastAPI(title="AI DA DBA")

# ---------------------------------------------------------------------------
# REST: list available databases
# ---------------------------------------------------------------------------

@app.get("/api/databases")
def api_databases():
    try:
        return {"databases": list_databases()}
    except Exception as e:
        log.error("Failed to list databases: %s", e)
        return {"databases": [], "error": str(e)}

# ---------------------------------------------------------------------------
# WebSocket: chat with agent
# ---------------------------------------------------------------------------

MAX_TOOL_ROUNDS = 10


@app.websocket("/ws")
async def ws_chat(ws: WebSocket):
    await ws.accept()
    messages: list[dict] = []
    database: str | None = None

    try:
        while True:
            raw = await ws.receive_text()
            payload = json.loads(raw)

            if payload.get("type") == "set_database":
                database = payload["database"]
                await ws.send_text(json.dumps({
                    "type": "system",
                    "content": f"Connected to database: {database}",
                }))
                messages.clear()
                continue

            if payload.get("type") == "message":
                if not database:
                    await ws.send_text(json.dumps({
                        "type": "error",
                        "content": "Please select a database first.",
                    }))
                    continue

                user_text = payload.get("content", "")
                messages.append({"role": "user", "content": user_text})

                await _agent_loop(ws, messages, database)

    except WebSocketDisconnect:
        log.info("Client disconnected")
    except Exception as e:
        log.exception("WebSocket error")
        try:
            await ws.send_text(json.dumps({"type": "error", "content": str(e)}))
        except Exception:
            pass


async def _agent_loop(ws: WebSocket, messages: list[dict], database: str):
    # setup messages for the current loop
    full_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    for round_num in range(MAX_TOOL_ROUNDS):
        log.info("Agent round %d, messages: %d", round_num + 1, len(full_messages))

        try:
            # enable streaming in llm client
            response = llm_client.chat.completions.create(
                model=LLM_MODEL,
                messages=full_messages,
                tools=TOOL_DEFINITIONS,
                temperature=0.2,
                stream=True, 
            )
        except Exception as e:
            log.error("LLM call failed: %s", e)
            await ws.send_text(json.dumps({"type": "error", "content": f"LLM error: {e}"}))
            return

        collected_msg = ""
        tools_acc = {}

        # process chunks as they arrive
        for chunk in response:
            delta = chunk.choices[0].delta

            # stream normal text directly to frontend
            if delta.content:
                collected_msg += delta.content
                await ws.send_text(json.dumps({
                    "type": "stream",
                    "content": delta.content
                }))

            # accumulate tool call chunks
            if delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index
                    if idx not in tools_acc:
                        tools_acc[idx] = {
                            "id": tc.id or "",
                            "type": "function",
                            "function": {"name": "", "arguments": ""}
                        }
                    if tc.function.name:
                        tools_acc[idx]["function"]["name"] += tc.function.name
                    if tc.function.arguments:
                        tools_acc[idx]["function"]["arguments"] += tc.function.arguments
                    if tc.id:
                        tools_acc[idx]["id"] += tc.id

        # if tools were called, execute them and continue the loop
        if tools_acc:
            assistant_msg = {
                "role": "assistant", 
                "content": collected_msg or None, 
                "tool_calls": list(tools_acc.values())
            }
            full_messages.append(assistant_msg)

            for tc in assistant_msg["tool_calls"]:
                t_name = tc["function"]["name"]
                try:
                    t_args = json.loads(tc["function"]["arguments"])
                except json.JSONDecodeError:
                    t_args = {}

                log.info("Tool call: %s(%s)", t_name, t_args)
                await ws.send_text(json.dumps({
                    "type": "tool_call",
                    "tool": t_name,
                    "args": t_args,
                }))

                result = dispatch_tool(t_name, t_args, database)
                full_messages.append({
                    "role": "tool",
                    "tool_call_id": tc["id"],
                    "content": result,
                })
            
            # go to the next react loop iteration
            continue

        # no tools called, meaning this is the final answer
        messages.append({"role": "assistant", "content": collected_msg})
        await ws.send_text(json.dumps({"type": "stream_end"}))
        return

    # safety limit reached
    err_msg = "Agent reached maximum tool call rounds."
    await ws.send_text(json.dumps({"type": "error", "content": err_msg}))


# ---------------------------------------------------------------------------
# Serve frontend static files
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory="../frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8888, reload=True)
