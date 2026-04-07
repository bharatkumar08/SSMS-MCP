"""
MCP Server - HTTP transport layer for the SQL tools.
Implements a minimal JSON-RPC 2.0 / MCP-compatible endpoint.
Runs on its own port so the Flask app can call it as a sidecar.
"""

import json
import logging
import os
from typing import Any

from dotenv import load_dotenv
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
import uvicorn

load_dotenv()

from mcp_server.tools import TOOLS, call_tool  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MCP] %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ── MCP JSON-RPC handlers ─────────────────────────────────────────────────────

def _ok(result: Any, req_id: Any = None) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _err(code: int, message: str, req_id: Any = None) -> dict:
    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}


async def handle_rpc(request: Request) -> JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(_err(-32700, "Parse error"), status_code=400)

    method = body.get("method", "")
    params = body.get("params", {})
    req_id = body.get("id")

    logger.info("RPC %s id=%s", method, req_id)

    # ── initialize ────────────────────────────────────────────────────────────
    if method == "initialize":
        return JSONResponse(_ok({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "sql-mcp-server", "version": "1.0.0"},
        }, req_id))

    # ── tools/list ────────────────────────────────────────────────────────────
    if method == "tools/list":
        return JSONResponse(_ok({"tools": TOOLS}, req_id))

    # ── tools/call ────────────────────────────────────────────────────────────
    if method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})
        if not tool_name:
            return JSONResponse(_err(-32602, "Missing tool name"), status_code=400)

        result = call_tool(tool_name, arguments)
        return JSONResponse(_ok({
            "content": [{"type": "text", "text": json.dumps(result, default=str)}],
            "isError": not result.get("success", True),
        }, req_id))

    # ── health ────────────────────────────────────────────────────────────────
    if method == "ping":
        return JSONResponse(_ok("pong", req_id))

    return JSONResponse(_err(-32601, f"Method not found: {method}"), status_code=404)


async def health(_: Request) -> JSONResponse:
    return JSONResponse({"status": "ok", "server": "sql-mcp-server"})


# ── Starlette app ─────────────────────────────────────────────────────────────

app = Starlette(routes=[
    Route("/", health),
    Route("/health", health),
    Route("/rpc", handle_rpc, methods=["POST"]),
    Route("/mcp", handle_rpc, methods=["POST"]),
])


def run():
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8001"))
    logger.info("Starting MCP server on %s:%s", host, port)
    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    run()
