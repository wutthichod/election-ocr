# src/election_ocr/correct.py
"""
Correction loop: re-extract (LLM) and optionally re-OCR pages for records
that fail validation. Attempt 1 uses LLM feedback only; attempts 2+ also
clear the OCR cache and re-OCR the source images before re-extracting.
"""
import asyncio
import pandas as pd
from pathlib import Path

from .config import settings
from .logging import log
from .ocr_client import TyphoonOCRClient
from .extract import re_extract_for_shas
from .validate import validate_518, validate_partylist
from .schemas.form_518 import Form518
from .schemas.form_partylist import FormPartylist


def get_issues(form_type: str, data: dict) -> list[str]:
    """Validate a raw data dict and return its issue list."""
    try:
        if form_type in ("5_18", "5_17"):
            return validate_518(Form518(**data)).issues
        else:
            return validate_partylist(FormPartylist(**data)).issues
    except Exception as e:
        return [str(e)[:200]]


async def _re_ocr_sha(sha256: str, pages_df: pd.DataFrame, client: TyphoonOCRClient) -> None:
    """Delete OCR cache for every page of sha256 and re-OCR from the source image."""
    pages = pages_df[pages_df.sha256 == sha256]
    out_base = settings.bronze_dir / "markdown"
    tasks = []
    for row in pages.itertuples():
        cache = out_base / row.sha256[:2] / row.sha256 / f"page_{int(row.page_num):02d}.md"
        cache.unlink(missing_ok=True)
        tasks.append(client.ocr_image(Path(row.image_path), cache_path=cache))
    try:
        await asyncio.gather(*tasks)
        log.info("re_ocr_done", sha=sha256[:12], pages=len(tasks))
    except Exception as e:
        log.warning("re_ocr_partial_fail", sha=sha256[:12], error=str(e)[:100])


async def correction_loop(
    quarantine: list[dict],
    max_attempts: int = 3,
) -> tuple[list[dict], list[dict]]:
    """
    Iteratively correct quarantined records.

    quarantine items: {sha256, form_type, issues: list[str], data: dict}

    Returns (recovered, still_bad) with the same structure.
    Recovered items have updated 'data' that passes validation.
    """
    if not quarantine:
        return [], []

    remaining = list(quarantine)
    recovered = []

    pages_df = pd.read_parquet(settings.data_root / "pages.parquet")
    pages_df = pages_df[pages_df.image_path.notna()]

    client = TyphoonOCRClient(
        base_url=settings.vllm_url,
        model=settings.ocr_model,
        api_key=settings.ocr_api_key,
        concurrency=min(settings.ocr_concurrency, 4),
        timeout_s=settings.ocr_timeout_s,
    )

    try:
        for attempt in range(max_attempts):
            if not remaining:
                break

            log.info("correction_attempt", attempt=attempt + 1, count=len(remaining))

            if attempt > 0:
                # Re-OCR all failing SHA256s so the LLM sees fresh OCR output
                unique_shas = list({r["sha256"] for r in remaining})
                log.info("re_ocr_batch", shas=len(unique_shas))
                await asyncio.gather(*[
                    _re_ocr_sha(sha, pages_df, client) for sha in unique_shas
                ])

            # Re-extract with per-record error feedback
            pairs = [(r["sha256"], r["form_type"], r.get("issues", [])) for r in remaining]
            new_recs = await re_extract_for_shas(pairs)

            next_remaining = []
            for new_rec in new_recs:
                sha, ft = new_rec["sha256"], new_rec["form_type"]
                orig = next(
                    (r for r in remaining if r["sha256"] == sha and r["form_type"] == ft),
                    {}
                )
                if new_rec["status"] != "ok":
                    next_remaining.append({
                        **orig,
                        "issues": [new_rec.get("error", "extraction failed")],
                    })
                    continue

                issues = get_issues(ft, new_rec["data"])
                if issues:
                    next_remaining.append({
                        "sha256": sha, "form_type": ft,
                        "issues": issues, "data": new_rec["data"],
                    })
                else:
                    recovered.append(new_rec)

            log.info(
                "correction_progress",
                attempt=attempt + 1,
                recovered_this_round=len(remaining) - len(next_remaining),
                still_bad=len(next_remaining),
            )
            remaining = next_remaining

    finally:
        await client.close()

    return recovered, remaining
