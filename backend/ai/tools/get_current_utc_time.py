from datetime import datetime, timezone


def get_current_utc_time() -> str:
    return datetime.now(timezone.utc).isoformat()


definition = {
    "type": "function",
    "function": {
        "name": "get_current_utc_time",
        "description": "Get the current UTC time.",
        "parameters": {},
    },
}
