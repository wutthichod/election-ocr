# src/election_ocr/extract.py
import asyncio
import instructor
from openai import AsyncOpenAI
from pathlib import Path
import pandas as pd
from .config import settings
from .schemas.form_518 import Form518
from .logging import log
from tenacity import retry, stop_after_attempt, wait_exponential

# instructor gives type-safe structured outputs against the OpenAI-compatible API
_client = instructor.from_openai(
    AsyncOpenAI(
        base_url=settings.extractor_base_url,
        api_key=settings.extractor_api_key,
    ),
    mode=instructor.Mode.JSON,
)

SYSTEM_518 = """You extract Thai election form ส.ส.5/18 data into strict JSON.

Critical rules:
- Every number appears as BOTH Arabic digits and Thai words. Read both.
- If digits and Thai words disagree, prefer the Thai word and copy it exactly.
- Candidate numbers (หมายเลขประจำตัวผู้สมัคร) must be sequential integers.
- Ignore signatures, scribbles, and struck-through text.
- election_date must be ISO format YYYY-MM-DD.
"""

@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
async def extract_518(markdown: str, sha256: str, pages: list[int]) -> Form518:
    result = await _client.chat.completions.create(
        model=settings.extractor_model,
        response_model=Form518,
        temperature=0.0,
        max_retries=2,  # instructor retries on validation failure
        messages=[
            {"role": "system", "content": SYSTEM_518},
            {"role": "user", "content": markdown},
        ],
    )
    # Stamp provenance
    result.source_sha256 = sha256
    result.source_pages = pages
    result.extractor_model = settings.extractor_model
    return result

async def extract_all():
    """Group pages by station, concatenate markdown, extract once per station."""
    ocr_df = pd.read_parquet(settings.data_root / "ocr_results.parquet")
    ocr_df = ocr_df[ocr_df.status == "ok"]
    manifest = pd.read_parquet(settings.data_root / "manifest.parquet")

    # Join so we know form_type
    merged = ocr_df.merge(manifest[["sha256", "form_type", "station_id"]], on="sha256")

    sem = asyncio.Semaphore(settings.extractor_concurrency)
    results = []

    async def process_station(sha, form_type, group):
        async with sem:
            pages = sorted(group.page_num.tolist())
            markdown = "\n\n".join(
                Path(p).read_text(encoding="utf-8")
                for p in group.sort_values("page_num").markdown_path
            )
            try:
                if form_type == "5_18":
                    obj = await extract_518(markdown, sha, pages)
                # elif form_type == "5_18_partylist": ...
                else:
                    return None
                return {"sha256": sha, "form_type": form_type,
                        "data": obj.model_dump(mode="json"), "status": "ok"}
            except Exception as e:
                log.error("extract_failed", sha=sha[:12], error=str(e))
                return {"sha256": sha, "form_type": form_type,
                        "status": "error", "error": str(e)[:300]}

    tasks = [
        process_station(sha, ft, g)
        for (sha, ft), g in merged.groupby(["sha256", "form_type"])
    ]
    results = [r for r in await asyncio.gather(*tasks) if r]

    # Write silver layer + quarantine
    ok = [r for r in results if r["status"] == "ok"]
    bad = [r for r in results if r["status"] == "error"]

    pd.DataFrame(ok).to_parquet(settings.silver_dir / "extracted.parquet", index=False)
    if bad:
        pd.DataFrame(bad).to_parquet(settings.quarantine_dir / "extract_failures.parquet", index=False)

    log.info("extract_done", ok=len(ok), errors=len(bad))