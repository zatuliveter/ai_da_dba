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

app = FastAPI(title="AI DBA Agent")

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
    """Run the ReAct agent loop: LLM -> tool calls -> LLM -> ... -> final answer."""

    full_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    for round_num in range(MAX_TOOL_ROUNDS):
        log.info("Agent round %d, messages: %d", round_num + 1, len(full_messages))

        try:
            response = llm_client.chat.completions.create(
                model=LLM_MODEL,
                messages=full_messages,
                tools=TOOL_DEFINITIONS,
                temperature=0.2,
            )
        except Exception as e:
            log.error("LLM call failed: %s", e)
            await ws.send_text(json.dumps({
                "type": "error",
                "content": f"LLM error: {e}",
            }))
            return

        choice = response.choices[0]
        msg = choice.message

        if msg.tool_calls:
            assistant_msg = {"role": "assistant", "content": msg.content, "tool_calls": []}
            for tc in msg.tool_calls:
                assistant_msg["tool_calls"].append({
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                })
            full_messages.append(assistant_msg)

            for tc in msg.tool_calls:
                tool_name = tc.function.name
                try:
                    tool_args = json.loads(tc.function.arguments)
                except json.JSONDecodeError:
                    tool_args = {}

                log.info("Tool call: %s(%s)", tool_name, tool_args)

                await ws.send_text(json.dumps({
                    "type": "tool_call",
                    "tool": tool_name,
                    "args": tool_args,
                }))

                result = dispatch_tool(tool_name, tool_args, database)

                full_messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": result,
                })

            continue

        # No tool calls — final answer
        final_text = msg.content or ""
        messages.append({"role": "assistant", "content": final_text})

        await ws.send_text(json.dumps({
            "type": "answer",
            "content": final_text,
        }))
        return

    # Safety: max rounds exceeded
    await ws.send_text(json.dumps({
        "type": "error",
        "content": "Agent reached maximum tool call rounds. Please try a simpler query.",
    }))


# ---------------------------------------------------------------------------
# Serve frontend static files
# ---------------------------------------------------------------------------

app.mount("/", StaticFiles(directory="../frontend", html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
