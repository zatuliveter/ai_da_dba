import json
import logging

from fastapi import WebSocket
from openai import OpenAI

from backend.config import API_KEY, API_URL, LLM_MODEL
from backend.ai.prompts import get_system_prompt
from backend.ai.store import ChatMessage, append_chat_messages, get_chat_messages, get_chat_token_stats, get_db_description
from backend.ai.tools import TOOL_DEFINITIONS, dispatch_tool

log = logging.getLogger(__name__)
    
llm_client = OpenAI(api_key=API_KEY, base_url=API_URL)
    
EMPTY_TOKEN_STATS: dict = {
    "last_prompt_tokens": 0,
    "total_prompt_tokens": 0,
    "total_cached_tokens": 0,
    "total_completion_tokens": 0,
}

MAX_TOOL_ROUNDS = 10
MAX_TOOL_RESULT_LENGTH = 80_000


def _parse_tool_args(tc: dict) -> dict:
    """Parse tool call arguments JSON. Returns {} on decode error."""
    try:
        return json.loads(tc["function"]["arguments"])
    except (json.JSONDecodeError, KeyError, TypeError):
        return {}


def _format_tool_call_content(name: str, args: dict) -> str:
    """Format tool name and args as a single string for chat display, e.g. get_indexes(table_name=Companies, schema=cab)."""
    parts = [f"{k}={v}" for k, v in (args or {}).items()]
    return f"{name}({', '.join(parts)})"


def _extract_cached_tokens(usage_data: dict) -> int | None:
    """Best-effort extraction of cached token count across Gemini/OpenAI-compatible shapes."""
    if not isinstance(usage_data, dict):
        return None
    candidates = [
        usage_data.get("cachedContentTokenCount"),
        usage_data.get("cached_content_token_count"),
        (usage_data.get("usageMetadata") or {}).get("cachedContentTokenCount"),
        (usage_data.get("prompt_tokens_details") or {}).get("cached_tokens"),
        (usage_data.get("input_tokens_details") or {}).get("cached_tokens"),
    ]
    for value in candidates:
        if isinstance(value, int):
            return value
    return None


def _extract_prompt_tokens(usage_data: dict | None) -> int | None:
    if not isinstance(usage_data, dict):
        return None
    um = usage_data.get("usageMetadata") or {}
    candidates = [
        usage_data.get("prompt_tokens"),
        usage_data.get("input_tokens"),
        um.get("promptTokenCount"),
    ]
    for value in candidates:
        if isinstance(value, int):
            return value
    return None


def _extract_completion_tokens(usage_data: dict | None) -> int | None:
    if not isinstance(usage_data, dict):
        return None
    um = usage_data.get("usageMetadata") or {}
    candidates = [
        usage_data.get("completion_tokens"),
        usage_data.get("output_tokens"),
        um.get("candidatesTokenCount"),
    ]
    for value in candidates:
        if isinstance(value, int):
            return value
    return None


async def _send_chat_token_update(ws: WebSocket, chat_id: int) -> None:
    stats = get_chat_token_stats(chat_id)
    await ws.send_text(json.dumps({"type": "chat_tokens", "chat_id": chat_id, **stats}))


def chat_messages_to_api_messages(stored: list[ChatMessage]) -> list[dict]:
    """Map persisted chat rows to OpenAI-compatible message dicts (user, assistant, tool)."""
    api_messages: list[dict] = []
    for m in stored:
        if m.role == "user" or m.role == "system":
            api_messages.append({"role": m.role, "content": m.content or ""})
        elif m.role == "assistant" or m.role == "dba":
            msg = {"role": "assistant", "content": m.content or ""}
            if m.tool_calls:
                msg["tool_calls"] = [
                    {
                        "id": tc.get("id", ""),
                        "type": tc.get("type", "function"),
                        "function": {
                            "name": (tc.get("function") or {}).get("name", ""),
                            "arguments": (tc.get("function") or {}).get("arguments", ""),
                        },
                    }
                    for tc in (m.tool_calls or [])
                ]
            api_messages.append(msg)
        elif m.role == "tool_call" and m.tool_call_id and m.tool_result is not None:
            api_messages.append({
                "role": "tool",
                "tool_call_id": m.tool_call_id,
                "content": m.tool_result,
            })
        else:
            log.warning("Unknown message role: %s", m.role)
    return api_messages


async def run_agent_loop(
    ws: WebSocket,
    connection_id: int,
    database: str,
    agent_role: str,
    chat_id: int,
):
    description = get_db_description(connection_id, database) or ""
    db_context = f"\n\nYou are working with database: {database}."
    if description:
        db_context += f" User-provided context: {description}"
    db_context += "\n"
    system_content = get_system_prompt(agent_role) + db_context

    for round_num in range(MAX_TOOL_ROUNDS):
        stored = get_chat_messages(chat_id)
        full_messages: list[dict] = [
            {"role": "system", "content": system_content},
        ] + chat_messages_to_api_messages(stored)
        log.info("Agent round %d, messages: %d", round_num + 1, len(full_messages))

        try:
            response = llm_client.chat.completions.create(
                model=LLM_MODEL,
                messages=full_messages,
                tools=TOOL_DEFINITIONS,
                temperature=0.2,
                stream=True,
                stream_options={"include_usage": True},
            )
        except Exception as e:
            log.error("LLM call failed: %s", e)
            await ws.send_text(json.dumps({"type": "error", "content": f"LLM error: {e}"}))
            return

        collected_msg = ""
        tools_acc = {}
        next_auto_index = 0
        last_usage_data: dict | None = None

        for chunk in response:
            usage = getattr(chunk, "usage", None)
            if usage:
                usage_data = usage.model_dump() if hasattr(usage, "model_dump") else usage
                if isinstance(usage_data, dict):
                    last_usage_data = usage_data
                    log.info("LLM usage metadata: %s", usage_data)
                    cached_tokens = _extract_cached_tokens(usage_data)
                    if cached_tokens is None:
                        log.info("Gemini cache: MISS (cached tokens=0)")
                    else:
                        log.info("Gemini cached tokens=%d", cached_tokens)
                else:
                    log.info("WARN: LLM usage metadata (non-dict): %s", usage_data)

            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta

            if delta.content:
                collected_msg += delta.content
                await ws.send_text(json.dumps({
                    "type": "stream",
                    "content": delta.content
                }))

            if delta.tool_calls:
                for tc in delta.tool_calls:
                    idx = tc.index if isinstance(tc.index, int) and tc.index >= 0 else next_auto_index
                    if idx >= next_auto_index:
                        next_auto_index = idx + 1
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

        pt = _extract_prompt_tokens(last_usage_data)
        cached_tok = _extract_cached_tokens(last_usage_data) if isinstance(last_usage_data, dict) else None
        comp = _extract_completion_tokens(last_usage_data)

        if tools_acc:
            sorted_calls = [v for _, v in sorted(tools_acc.items())]
            to_append = [
                ChatMessage(
                    role="assistant",
                    content=collected_msg or "",
                    tool_calls=sorted_calls,
                    prompt_tokens=pt,
                    cached_tokens=cached_tok,
                    completion_tokens=comp,
                ),
            ]
            for tc in sorted_calls:
                t_name = tc["function"]["name"]
                t_args = _parse_tool_args(tc)
                log.info("Tool call: %s(%s)", t_name, t_args)
                await ws.send_text(json.dumps({
                    "type": "tool_call",
                    "tool": t_name,
                    "args": t_args,
                }))

                result = dispatch_tool(t_name, t_args, connection_id, database)
                if result is None or result == "":
                    result = "(tool returned no result)"
                if len(result) > MAX_TOOL_RESULT_LENGTH:
                    result = result[:MAX_TOOL_RESULT_LENGTH] + "\n\n[... result truncated due to size ...]"
                await ws.send_text(json.dumps({"type": "tool_result", "result": result}))
                to_append.append(
                    ChatMessage(
                        role="tool_call",
                        content=_format_tool_call_content(t_name, t_args),
                        tool_result=result,
                        tool_call_id=tc["id"],
                    )
                )
            append_chat_messages(chat_id, to_append)
            await _send_chat_token_update(ws, chat_id)
            continue

        append_chat_messages(chat_id, [
            ChatMessage(
                role=agent_role,
                content=collected_msg,
                prompt_tokens=pt,
                cached_tokens=cached_tok,
                completion_tokens=comp,
            ),
        ])
        await _send_chat_token_update(ws, chat_id)
        await ws.send_text(json.dumps({"type": "stream_end"}))
        return

    err_msg = "Agent reached maximum tool call rounds."
    await ws.send_text(json.dumps({"type": "error", "content": err_msg}))
