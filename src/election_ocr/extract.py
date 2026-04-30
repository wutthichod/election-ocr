# src/election_ocr/extract.py
import asyncio
import instructor
from openai import AsyncOpenAI
from pathlib import Path
import pandas as pd
from .config import settings
from .schemas.form_518 import Form518
from .schemas.form_partylist import FormPartylist
from .logging import log
from tenacity import retry, stop_after_attempt, wait_exponential

# instructor gives type-safe structured outputs against the OpenAI-compatible API
# Mode.JSON_SCHEMA uses response_format.json_schema (vLLM guided decoding) — Mode.JSON
# caused the model to echo the schema definition back instead of extracting data.
_client = instructor.from_openai(
    AsyncOpenAI(
        base_url=settings.extractor_base_url,
        api_key=settings.extractor_api_key,
    ),
    mode=instructor.Mode.JSON_SCHEMA,
)

SYSTEM_518 = """The document contains TWO tally forms merged together:
  1. ส.ส.5/18 — constituency (แบบแบ่งเขต): section 3 lists INDIVIDUAL CANDIDATES by name.
  2. ส.ส.5/18(บช) — party-list (บัญชีรายชื่อ): section 3 lists PARTIES.

YOUR TASK: extract ONLY form ส.ส.5/18 (the constituency form).
IGNORE everything belonging to the party-list form ส.ส.5/18(บช).

Critical rules:
- Section 3 must contain individual candidate names — if you see only party names, you are reading the wrong form.
- Every number appears as BOTH Arabic digits and Thai words. Read both.
- If digits and Thai words disagree, prefer the Thai word and copy it exactly.
- Candidate numbers (หมายเลขประจำตัวผู้สมัคร) must be sequential integers.
- Ignore signatures, scribbles, and struck-through text.
- election_date: convert Thai Buddhist Era (พ.ศ.) to CE by subtracting 543 (e.g. พ.ศ. 2569 → 2026-02-28).
- Before responding, verify: sum of all candidate votes_digit == ballots_valid == total_votes. If they don't match, recount and correct before returning JSON.

OUTPUT FORMAT — respond with JSON using EXACTLY these field names:
{"source_sha256":"","source_pages":[],"polling_station":<int>,"tambon":"<str>","amphoe":"<str>","changwat":"<str>","constituency":<int>,"election_date":"<YYYY-MM-DD CE>","voters_registered":<int>,"voters_present":<int>,"ballots_allocated":<int>,"ballots_used":<int>,"ballots_valid":<int>,"ballots_invalid":<int>,"ballots_no_vote":<int>,"ballots_remaining":<int>,"candidates":[{"number":<int>,"name":"<str>","party":"<str>","votes_digit":<int>,"votes_thai_word":"<str>"}],"total_votes":<int>,"extraction_confidence":null,"extractor_model":null}
"""

SYSTEM_517 = """The document contains TWO tally forms merged together:
  1. ส.ส.5/17 — advance constituency (แบบแบ่งเขต ล่วงหน้า): section 3 lists INDIVIDUAL CANDIDATES by name.
  2. ส.ส.5/17(บช) — advance party-list (บัญชีรายชื่อ ล่วงหน้า): section 3 lists PARTIES.

YOUR TASK: extract ONLY form ส.ส.5/17 (the advance constituency form).
IGNORE everything belonging to the advance party-list form ส.ส.5/17(บช).

Critical rules:
- Section 3 must contain individual candidate names — if you see only party names, you are reading the wrong form.
- Every number appears as BOTH Arabic digits and Thai words. Read both.
- If digits and Thai words disagree, prefer the Thai word and copy it exactly.
- Candidate numbers (หมายเลขประจำตัวผู้สมัคร) must be sequential integers.
- Ignore signatures, scribbles, and struck-through text.
- election_date: convert Thai Buddhist Era (พ.ศ.) to CE by subtracting 543 (e.g. พ.ศ. 2569 → 2026-02-28).
- Before responding, verify: sum of all candidate votes_digit == ballots_valid == total_votes. If they don't match, recount and correct before returning JSON.

OUTPUT FORMAT — respond with JSON using EXACTLY these field names:
{"source_sha256":"","source_pages":[],"polling_station":<int>,"tambon":"<str>","amphoe":"<str>","changwat":"<str>","constituency":<int>,"election_date":"<YYYY-MM-DD CE>","voters_registered":<int>,"voters_present":<int>,"ballots_allocated":<int>,"ballots_used":<int>,"ballots_valid":<int>,"ballots_invalid":<int>,"ballots_no_vote":<int>,"ballots_remaining":<int>,"candidates":[{"number":<int>,"name":"<str>","party":"<str>","votes_digit":<int>,"votes_thai_word":"<str>"}],"total_votes":<int>,"extraction_confidence":null,"extractor_model":null}
"""

SYSTEM_PARTYLIST = """The document contains TWO tally forms merged together:
  1. ส.ส.5/18 — constituency (แบบแบ่งเขต): section 3 lists INDIVIDUAL CANDIDATES by name.
  2. ส.ส.5/18(บช) — party-list (บัญชีรายชื่อ): section 3 lists PARTIES.

YOUR TASK: extract ONLY form ส.ส.5/18(บช) (the party-list form).
IGNORE everything belonging to the constituency form ส.ส.5/18.

Critical rules:
- Section 3 must contain PARTY names (พรรค) — if you see individual candidate names, you are reading the wrong form.
- Every number appears as BOTH Arabic digits and Thai words. Read both.
- If digits and Thai words disagree, prefer the Thai word and copy it exactly.
- Party numbers (หมายเลขพรรค) must be sequential integers.
- Ignore signatures, scribbles, and struck-through text.
- election_date: convert Thai Buddhist Era (พ.ศ.) to CE by subtracting 543 (e.g. พ.ศ. 2569 → 2026-02-28).
- Before responding, verify: sum of all party votes_digit == ballots_valid == total_votes. If they don't match, recount and correct before returning JSON.

OUTPUT FORMAT — respond with JSON using EXACTLY these field names:
{"source_sha256":"","source_pages":[],"polling_station":<int>,"tambon":"<str>","amphoe":"<str>","changwat":"<str>","constituency":<int>,"election_date":"<YYYY-MM-DD CE>","voters_registered":<int>,"voters_present":<int>,"ballots_allocated":<int>,"ballots_used":<int>,"ballots_valid":<int>,"ballots_invalid":<int>,"ballots_no_vote":<int>,"ballots_remaining":<int>,"parties":[{"number":<int>,"name":"<str>","votes_digit":<int>,"votes_thai_word":"<str>"}],"total_votes":<int>,"extraction_confidence":null,"extractor_model":null}
"""

SYSTEM_PARTYLIST_517 = """The document contains TWO tally forms merged together:
  1. ส.ส.5/17 — advance constituency (แบบแบ่งเขต ล่วงหน้า): section 3 lists INDIVIDUAL CANDIDATES by name.
  2. ส.ส.5/17(บช) — advance party-list (บัญชีรายชื่อ ล่วงหน้า): section 3 lists PARTIES.

YOUR TASK: extract ONLY form ส.ส.5/17(บช) (the advance party-list form).
IGNORE everything belonging to the advance constituency form ส.ส.5/17.

Critical rules:
- Section 3 must contain PARTY names (พรรค) — if you see individual candidate names, you are reading the wrong form.
- Every number appears as BOTH Arabic digits and Thai words. Read both.
- If digits and Thai words disagree, prefer the Thai word and copy it exactly.
- Party numbers (หมายเลขพรรค) must be sequential integers.
- Ignore signatures, scribbles, and struck-through text.
- election_date: convert Thai Buddhist Era (พ.ศ.) to CE by subtracting 543 (e.g. พ.ศ. 2569 → 2026-02-28).
- Before responding, verify: sum of all party votes_digit == ballots_valid == total_votes. If they don't match, recount and correct before returning JSON.

OUTPUT FORMAT — respond with JSON using EXACTLY these field names:
{"source_sha256":"","source_pages":[],"polling_station":<int>,"tambon":"<str>","amphoe":"<str>","changwat":"<str>","constituency":<int>,"election_date":"<YYYY-MM-DD CE>","voters_registered":<int>,"voters_present":<int>,"ballots_allocated":<int>,"ballots_used":<int>,"ballots_valid":<int>,"ballots_invalid":<int>,"ballots_no_vote":<int>,"ballots_remaining":<int>,"parties":[{"number":<int>,"name":"<str>","votes_digit":<int>,"votes_thai_word":"<str>"}],"total_votes":<int>,"extraction_confidence":null,"extractor_model":null}
"""


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
async def _extract_constituency(system: str, markdown: str, sha256: str, pages: list[int]) -> Form518:
    result = await _client.chat.completions.create(
        model=settings.extractor_model,
        response_model=Form518,
        temperature=0.0,
        max_tokens=4096,
        max_retries=2,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": markdown},
        ],
    )
    result.source_sha256 = sha256
    result.source_pages = pages
    result.extractor_model = settings.extractor_model
    return result


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=20))
async def _extract_partylist(system: str, markdown: str, sha256: str, pages: list[int]) -> FormPartylist:
    result = await _client.chat.completions.create(
        model=settings.extractor_model,
        response_model=FormPartylist,
        temperature=0.0,
        max_tokens=4096,
        max_retries=2,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": markdown},
        ],
    )
    result.source_sha256 = sha256
    result.source_pages = pages
    result.extractor_model = settings.extractor_model
    return result


# Maps form_type → (system_prompt, extractor_fn)
# Each prompt explicitly names the target form and tells the LLM to ignore the other.
_EXTRACTOR_CFG = {
    "5_18":           (SYSTEM_518,           _extract_constituency),
    "5_17":           (SYSTEM_517,           _extract_constituency),
    "5_18_partylist": (SYSTEM_PARTYLIST,     _extract_partylist),
    "5_17_partylist": (SYSTEM_PARTYLIST_517, _extract_partylist),
}


async def extract_all():
    """For each polling station, extract all applicable tally forms in parallel.
    Which forms to extract is determined by manifest.form_types (detected from directory name)."""
    ocr_df = pd.read_parquet(settings.data_root / "ocr_results.parquet")
    ocr_df = ocr_df[ocr_df.status == "ok"]
    manifest = pd.read_parquet(settings.data_root / "manifest.parquet")

    merged = ocr_df.merge(manifest[["sha256", "station_id", "form_types"]], on="sha256")

    sem = asyncio.Semaphore(settings.extractor_concurrency)
    
    async def process_station(sha, group):
        async with sem:
            pages = sorted(group.page_num.tolist())
            markdown = "\n\n".join(
                Path(p).read_text(encoding="utf-8")
                for p in group.sort_values("page_num").markdown_path
            )
            form_types = group.iloc[0]["form_types"]  # list e.g. ["5_18", "5_18_partylist"]

            async def _run(label):
                system, fn = _EXTRACTOR_CFG[label]
                try:
                    obj = await fn(system, markdown, sha, pages)
                    return {"sha256": sha, "form_type": label,
                            "data": obj.model_dump(mode="json"), "status": "ok"}
                except Exception as e:
                    log.error("extract_failed", sha=sha[:12], form_type=label, error=str(e))
                    return {"sha256": sha, "form_type": label,
                            "status": "error", "error": str(e)[:300]}

            return list(await asyncio.gather(*[_run(ft) for ft in form_types]))

    station_results = await asyncio.gather(*[
        process_station(sha, g) for sha, g in merged.groupby("sha256")
    ])
    results = [rec for pair in station_results for rec in pair]

    ok = [r for r in results if r["status"] == "ok"]
    bad = [r for r in results if r["status"] == "error"]

    pd.DataFrame(ok).to_parquet(settings.silver_dir / "extracted.parquet", index=False)
    if bad:
        pd.DataFrame(bad).to_parquet(settings.quarantine_dir / "extract_failures.parquet", index=False)

    log.info("extract_done", ok=len(ok), errors=len(bad))


async def re_extract_for_shas(pairs: list[tuple[str, str, list[str]]]) -> list[dict]:
    """Re-extract specific (sha256, form_type, prior_errors) triples — used by load to retry validation failures.
    prior_errors is injected into the user message so the LLM knows what to correct."""
    ocr_df = pd.read_parquet(settings.data_root / "ocr_results.parquet")
    ocr_df = ocr_df[ocr_df.status == "ok"]

    sem = asyncio.Semaphore(settings.extractor_concurrency)

    async def _one(sha: str, form_type: str, prior_errors: list[str]) -> dict:
        async with sem:
            group = ocr_df[ocr_df.sha256 == sha].sort_values("page_num")
            if group.empty:
                return {"sha256": sha, "form_type": form_type,
                        "status": "error", "error": "no OCR rows found"}
            pages = sorted(group.page_num.tolist())
            markdown = "\n\n".join(
                Path(p).read_text(encoding="utf-8") for p in group.markdown_path
            )
            if prior_errors:
                error_block = "PREVIOUS EXTRACTION ERRORS (fix these before responding):\n"
                error_block += "\n".join(f"- {e}" for e in prior_errors)
                user_content = f"{error_block}\n---\n{markdown}"
            else:
                user_content = markdown
            system, fn = _EXTRACTOR_CFG[form_type]
            try:
                obj = await fn(system, user_content, sha, pages)
                log.info("re_extract_ok", sha=sha[:12], form_type=form_type)
                return {"sha256": sha, "form_type": form_type,
                        "data": obj.model_dump(mode="json"), "status": "ok"}
            except Exception as e:
                log.warning("re_extract_failed", sha=sha[:12], form_type=form_type, error=str(e)[:200])
                return {"sha256": sha, "form_type": form_type,
                        "status": "error", "error": str(e)[:300]}

    return list(await asyncio.gather(*[_one(sha, ft, errs) for sha, ft, errs in pairs]))
