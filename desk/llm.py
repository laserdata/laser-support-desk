"""The LLM seam: claude -p by default, the Anthropic API if a key is set, or a mock.

DESK_LLM=claude|api|mock forces one. Every answer is kept in `.last` so the
lanes can show it."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import urllib.error
import urllib.request


class MockLlm:
    name = "mock"
    last = ""

    async def complete(self, prompt: str) -> str:
        self.last = f"[mock-llm] {prompt.strip().splitlines()[0][:90]}"
        return self.last


class ClaudeCliLlm:
    """Claude Code in print mode: no API key, uses your existing login."""
    last = ""

    def __init__(self, path: str) -> None:
        self.path = path
        self.name = "claude -p"

    async def complete(self, prompt: str) -> str:
        proc = await asyncio.create_subprocess_exec(
            self.path, "-p", prompt + "\n\nAnswer briefly, no preamble.",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=180)
        except asyncio.TimeoutError:
            proc.kill()
            self.last = "[claude -p timed out]"
            return self.last
        text = out.decode().strip() or f"[claude -p error] {err.decode().strip()[:200]}"
        self.last = " ".join(text.split())
        return text


class AnthropicLlm:
    last = ""

    def __init__(self, key: str) -> None:
        self.key = key
        self.model = os.environ.get("ANTHROPIC_MODEL", "claude-sonnet-4-6")
        self.name = f"Anthropic API ({self.model})"

    async def complete(self, prompt: str) -> str:
        body = json.dumps({"model": self.model, "max_tokens": 400,
                           "messages": [{"role": "user", "content": prompt}]}).encode()
        req = urllib.request.Request("https://api.anthropic.com/v1/messages", data=body, headers={
            "content-type": "application/json", "x-api-key": self.key,
            "anthropic-version": "2023-06-01"})

        def call() -> str:
            try:
                with urllib.request.urlopen(req, timeout=60) as resp:
                    data = json.loads(resp.read())
                return "".join(b.get("text", "") for b in data.get("content", []))
            except (urllib.error.URLError, ValueError) as error:
                return f"[llm error] {error}"
        text = await asyncio.to_thread(call)
        self.last = " ".join(text.split())
        return text


def choose_llm():
    choice = os.environ.get("DESK_LLM", "").strip().lower()
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    cli = shutil.which("claude")
    if choice == "mock":
        return MockLlm()
    if choice == "api" or (key and choice != "claude"):
        return AnthropicLlm(key) if key else MockLlm()
    if cli:
        return ClaudeCliLlm(cli)
    return MockLlm()


def parse_json(text: str, default: dict) -> dict:
    """The first {...} in an answer, merged over a rule-based default."""
    start, end = text.find("{"), text.rfind("}")
    try:
        parsed = json.loads(text[start:end + 1]) if start >= 0 and end > start else {}
    except ValueError:
        parsed = {}
    if not isinstance(parsed, dict):
        parsed = {}
    return {**default, **{k: v for k, v in parsed.items() if v is not None}}


def is_mock(text: str) -> bool:
    return text.startswith("[mock-llm]") or text.startswith("[claude -p") or text.startswith("[llm error]")
