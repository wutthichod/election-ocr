import asyncio
import base64
import io
import random
import time
import httpx
from pathlib import Path
from typing import Optional
from PIL import Image
from .config import settings

_MAX_PX = 2048   # longest edge sent to the API
_JPEG_Q = 85     # JPEG quality for the resized image

TYPHOON_OCR_PROMPT = """Extract all text from this Thai election form image.

Rules:
- Preserve the exact layout and structure
- Use markdown tables for tabular data (candidate lists with vote counts)
- Keep all Thai text exactly as shown
- Include all section numbers (1.1, 1.2, 2.1, 2.2, etc.)
- Return only the extracted text, no explanations or metadata"""

def _prepare_image(path: Path) -> str:
    """Resize to _MAX_PX longest edge, encode as JPEG base64."""
    Image.MAX_IMAGE_PIXELS = None  # election scans are legitimately large
    img = Image.open(path).convert("RGB")
    w, h = img.size
    if max(w, h) > _MAX_PX:
        scale = _MAX_PX / max(w, h)
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=_JPEG_Q)
    return base64.b64encode(buf.getvalue()).decode()


# typhoon-ocr limits per documentation: 20 RPM, 2 RPS
_OCR_RPM = 20
_OCR_MIN_GAP_S = 1.0 / 2  # 2 RPS → min 0.5 s between calls


class _RateLimiter:
    """Sliding-window rate limiter with optional minimum gap between calls."""

    def __init__(self, max_calls: int, period_s: float, min_gap_s: float = 0.0):
        self.max_calls = max_calls
        self.period_s = period_s
        self.min_gap_s = min_gap_s
        self._times: list[float] = []
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()

            # Drop timestamps outside the sliding window
            cutoff = now - self.period_s
            self._times = [t for t in self._times if t > cutoff]

            # If window is full, wait until oldest slot expires
            if len(self._times) >= self.max_calls:
                wait = self.period_s - (now - self._times[0])
                if wait > 0:
                    await asyncio.sleep(wait)
                    now = time.monotonic()

            # Enforce minimum gap between consecutive calls
            if self._times and self.min_gap_s > 0:
                gap = now - self._times[-1]
                if gap < self.min_gap_s:
                    await asyncio.sleep(self.min_gap_s - gap)
                    now = time.monotonic()

            self._times.append(time.monotonic())


class TyphoonOCRClient:
    """Async OpenAI-compatible client for Typhoon OCR with rate limiting."""

    def __init__(
        self,
        base_url: str,
        model: str,
        api_key: str,
        concurrency: int,
        timeout_s: int = 120,
    ):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.sem = asyncio.Semaphore(concurrency)
        self._rl = _RateLimiter(
            max_calls=_OCR_RPM,
            period_s=60.0,
            min_gap_s=_OCR_MIN_GAP_S,
        )

        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
            headers["Content-Type"] = "application/json"

        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(timeout_s),
            limits=httpx.Limits(
                max_connections=concurrency * 2,
                max_keepalive_connections=concurrency,
            ),
            headers=headers,
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def _call(self, image_b64: str, max_retries: int = 5) -> str:
        payload = {
            "model": self.model,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{image_b64}"}},
                    {"type": "text", "text": TYPHOON_OCR_PROMPT},
                ],
            }],
            "max_tokens": 4096,
            "temperature": 0.0,
        }

        for attempt in range(max_retries + 1):
            await self._rl.acquire()
            try:
                r = await self.client.post(f"{self.base_url}/chat/completions", json=payload)
                r.raise_for_status()
                return r.json()["choices"][0]["message"]["content"]

            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                if status == 429 and attempt < max_retries:
                    # Exponential backoff + jitter on rate limit hit
                    backoff = min(2 ** (attempt + 1) + random.random(), 60.0)
                    await asyncio.sleep(backoff)
                    continue
                print(f"\n=== API ERROR ===")
                print(f"Status: {status}")
                print(f"Response: {e.response.text}")
                print(f"Model: {self.model}  URL: {self.base_url}/chat/completions")
                raise

            except (httpx.ReadTimeout, httpx.ConnectError, httpx.RemoteProtocolError) as e:
                if attempt < max_retries:
                    backoff = min(2 ** attempt + random.random(), 30.0)
                    await asyncio.sleep(backoff)
                    continue
                raise

        raise RuntimeError("OCR max retries exceeded")

    async def ocr_image(self, image_path: Path, cache_path: Optional[Path] = None) -> str:
        """OCR one image with cache-first lookup."""
        if cache_path and cache_path.exists() and cache_path.stat().st_size > 50:
            return cache_path.read_text(encoding="utf-8")

        async with self.sem:
            image_b64 = _prepare_image(image_path)
            markdown = await self._call(image_b64)

        if cache_path:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(markdown, encoding="utf-8")

        return markdown
