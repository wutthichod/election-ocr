# src/election_ocr/ocr_client.py
import asyncio
import httpx
from pathlib import Path
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import base64
from .config import settings
from .logging import log

# Typhoon OCR v1.5 expects its own prompt. We call the OpenAI-compatible
# endpoint directly to control concurrency (bypassing the typhoon_ocr wrapper).
TYPHOON_PROMPT = """Below is an image of a document page. Extract the text content from this image accurately while preserving the layout. Include markdown tables for tabular data. Return only the extracted content."""

class TyphoonOCRClient:
    """Async client for Typhoon OCR v1.5 over vLLM's OpenAI-compatible API.

    Uses a semaphore for backpressure — N concurrent requests max.
    Retries with exponential backoff on transient errors.
    Content-addressed caching: same image hash → cached markdown.
    """

    def __init__(self, base_url: str, model: str, concurrency: int,
                 timeout_s: int = 120):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.sem = asyncio.Semaphore(concurrency)
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_s),
            limits=httpx.Limits(max_connections=concurrency * 2,
                                max_keepalive_connections=concurrency),
        )

    async def close(self):
        await self.client.aclose()

    @retry(
        stop=stop_after_attempt(settings.ocr_max_retries),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((httpx.TimeoutException, httpx.HTTPStatusError)),
        reraise=True,
    )
    async def _call(self, image_b64: str) -> str:
        payload = {
            "model": self.model,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": TYPHOON_PROMPT},
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/png;base64,{image_b64}"}},
                ],
            }],
            "max_tokens": 8192,
            "temperature": 0.0,
        }
        r = await self.client.post(f"{self.base_url}/chat/completions", json=payload)
        r.raise_for_status()
        return r.json()["choices"][0]["message"]["content"]

    async def ocr_image(self, image_path: Path, cache_path: Optional[Path] = None) -> str:
        """OCR one image. Returns markdown. Resumable via cache_path."""
        if cache_path and cache_path.exists() and cache_path.stat().st_size > 50:
            return cache_path.read_text(encoding="utf-8")

        async with self.sem:
            image_b64 = base64.b64encode(image_path.read_bytes()).decode()
            markdown = await self._call(image_b64)

        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(markdown, encoding="utf-8")
        return markdown